import time
from datetime import datetime, timedelta
import pytz
import requests
import os
from dotenv import load_dotenv
import telegram
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from binance.client import Client
from binance.enums import *
from binance.exceptions import BinanceAPIException

# --- Load environment variables ---
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
# Support multiple chat IDs (comma or space separated)
TELEGRAM_CHAT_IDS = os.getenv("TELEGRAM_CHAT_ID", "").replace(',', ' ').split()
BINANCE_API_KEY = os.getenv("API_KEY")
BINANCE_API_SECRET = os.getenv("API_SECRET")
IST = pytz.timezone("Asia/Kolkata")

# Whitelisted IPs (comma or space separated, set in your .env file)
WHITELISTED_IPS = os.getenv("WHITELISTED_IPS", "").replace(',', ' ').split()

# FUNDING RATE THRESHOLD (-0.5% as decimal); change here for future updates
FUNDING_RATE_THRESHOLD = -0.005
# INITIAL CAPITAL IN USDT
INITIAL_TRADE_USDT = 50
# NEXT TRADE CAPITAL % of available USDT after closing previous trade
NEXT_TRADE_CAPITAL_PCT = 0.99
# LEVERAGE
TRADE_LEVERAGE = 1
# STOP LOSS percent below entry
STOP_LOSS_PCT = 0.10

client = Client(BINANCE_API_KEY, BINANCE_API_SECRET, testnet=False)

def get_public_ip():
    try:
        response = requests.get("https://api.ipify.org?format=json", timeout=10)
        response.raise_for_status()
        ip = response.json().get("ip")
        return ip
    except Exception as e:
        log_error(f"Failed to fetch public IP: {e}")
        return None

def verify_ip_whitelist():
    ip = get_public_ip()
    log_info(f"Detected Public IP: {ip}")
    if not ip:
        log_error("Could not determine public IP for whitelist verification.")
        send_telegram_to_all("❌ <b>Bot Startup Failed</b>\nReason: Public IP could not be determined.\nCheck network connection.")
        exit(1)
    if WHITELISTED_IPS and ip not in WHITELISTED_IPS:
        log_error(f"Public IP {ip} is NOT in whitelist: {WHITELISTED_IPS}")
        send_telegram_to_all(f"❌ <b>Bot Startup Failed</b>\nReason: Public IP <b>{ip}</b> not whitelisted.\nAllowed IPs: <b>{', '.join(WHITELISTED_IPS)}</b>")
        exit(1)
    log_info("Public IP is whitelisted. Bot will continue.")
    send_telegram_to_all(f"✅ <b>IP Whitelist Check Passed</b>\nPublic IP: <b>{ip}</b>\nAllowed IPs: <b>{', '.join(WHITELISTED_IPS)}</b>")

def log_info(msg):
    timestamp = datetime.now(IST).strftime("[%Y-%m-%d %H:%M:%S IST]")
    print(f"{timestamp} [INFO] {msg}")

def log_error(msg):
    timestamp = datetime.now(IST).strftime("[%Y-%m-%d %H:%M:%S IST]")
    print(f"{timestamp} [ERROR] {msg}")

async def send_telegram_message(message):
    bot = telegram.Bot(token=TELEGRAM_TOKEN)
    for chat_id in TELEGRAM_CHAT_IDS:
        try:
            await bot.send_message(chat_id=chat_id, text=message, parse_mode="HTML")
            log_info(f"Telegram message sent to {chat_id}")
        except Exception as e:
            log_error(f"Failed to send Telegram message to {chat_id}: {e}")

def send_telegram_to_all(message):
    asyncio.run(send_telegram_message(message))

def format_time(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def format_time_remaining(seconds):
    seconds = int(round(seconds))
    mins, secs = divmod(seconds, 60)
    return f"{mins} mins {secs} seconds"

def get_binance_usdt_perpetual_symbols():
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    log_info(f"Fetching symbol list from Binance: {url}")
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        json_data = response.json()
        symbols = [
            s["symbol"]
            for s in json_data.get("symbols", [])
            if (
                s.get("contractType") == "PERPETUAL"
                and s.get("quoteAsset") == "USDT"
                and s.get("status") == "TRADING"
            )
        ]
        log_info(f"Fetched {len(symbols)} USDT-margined perpetual futures symbols.")
        return symbols
    except Exception as e:
        log_error(f"Failed to fetch symbol list: {e}")
        return []

def get_funding_data(symbol):
    url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        return {
            "symbol": symbol,
            "fundingRate": float(data.get("lastFundingRate", 0)),
            "nextFundingTime": int(data.get("nextFundingTime", 0)),  # ms
            "price": float(data.get("markPrice", 0)),
        }
    except Exception as e:
        log_error(f"Funding fetch for {symbol}: {e}")
        return {
            "symbol": symbol,
            "fundingRate": None,
            "nextFundingTime": None,
            "price": None,
            "error": str(e),
        }

def get_futures_balance():
    try:
        balance = client.futures_account_balance()
        usdt_balances = [float(b['balance']) for b in balance if b['asset'] == 'USDT']
        bal = usdt_balances[0] if usdt_balances else 0.0
        log_info(f"Current futures USDT balance: {bal}")
        return bal
    except Exception as e:
        log_error(f"Cannot fetch futures balance: {e}")
        return 0.0

def set_leverage(symbol, leverage):
    try:
        client.futures_change_leverage(symbol=symbol, leverage=leverage)
        log_info(f"Set leverage {leverage}x for {symbol}")
    except BinanceAPIException as e:
        log_error(f"Could not set leverage: {e}")

def place_market_long(symbol, qty):
    log_info(f"Attempting market BUY for {symbol} qty: {qty}")
    try:
        order = client.futures_create_order(
            symbol=symbol,
            side=SIDE_BUY,
            type=ORDER_TYPE_MARKET,
            quantity=qty
        )
        log_info(f"Market BUY successful for {symbol}, qty: {qty}")
        return order
    except BinanceAPIException as e:
        log_error(f"Order error: {e}")
        return None

def place_stop_loss(symbol, entry_price, qty):
    stop_price = round(entry_price * (1 - STOP_LOSS_PCT), 6)
    log_info(f"Placing stop loss at {stop_price} ({STOP_LOSS_PCT*100}% below entry price)")
    try:
        order = client.futures_create_order(
            symbol=symbol,
            side=SIDE_SELL,
            type=ORDER_TYPE_STOP_MARKET,
            stopPrice=stop_price,
            quantity=qty,
            reduceOnly=True
        )
        log_info(f"Stop loss order placed at {stop_price}")
        return order
    except BinanceAPIException as e:
        log_error(f"Failed to place stop loss: {e}")
        return None

def close_market_long(symbol, qty):
    log_info(f"Attempting market SELL (close) for {symbol} qty: {qty}")
    try:
        order = client.futures_create_order(
            symbol=symbol,
            side=SIDE_SELL,
            type=ORDER_TYPE_MARKET,
            quantity=qty,
            reduceOnly=True
        )
        log_info(f"Market SELL successful for {symbol}, qty: {qty}")
        return order
    except BinanceAPIException as e:
        log_error(f"Order error: {e}")
        return None

def truncate_qty(price, capital):
    if price <= 0: 
        log_error(f"Price <= 0 for qty calc, returning 0")
        return 0
    qty = int(capital / price)
    log_info(f"Calculated qty: {qty} for capital: {capital} and price: {price}")
    return qty if qty > 0 else 0

def scan_opportunities():
    log_info("Scanning funding rate opportunities on Binance USDT-margined perpetual futures...")
    start_time = time.time()
    symbols = get_binance_usdt_perpetual_symbols()
    num_scanned = len(symbols)
    log_info(f"Total pairs scanned: {num_scanned}")

    # Trade state
    trade_active = False
    current_trade = {}
    trade_planned = False

    funding_results = []
    log_info("Fetching funding rates and prices in parallel...")
    with ThreadPoolExecutor(max_workers=30) as executor:
        future_to_symbol = {executor.submit(get_funding_data, symbol): symbol for symbol in symbols}
        for idx, future in enumerate(as_completed(future_to_symbol), 1):
            funding = future.result()
            funding_results.append(funding)
            rate = funding.get("fundingRate")
            funding_timestamp = funding.get("nextFundingTime")
            price = funding.get("price")
            symbol = funding.get("symbol")
            if rate is not None and funding_timestamp is not None and price is not None:
                funding_time_utc = funding_timestamp / 1000
                funding_time_utc = datetime.fromtimestamp(funding_time_utc, tz=pytz.UTC)
                funding_time_ist = funding_time_utc.astimezone(IST)
                now_ist = datetime.now(IST)
                time_to_funding = (funding_time_ist - now_ist).total_seconds()
                time_remaining_str = format_time_remaining(time_to_funding) if time_to_funding > 0 else "Ended"
                log_info(f"[{idx}/{num_scanned}] {symbol} | Price: {price} | FundingRate: {rate:.4f} | FundingEnds(IST): {format_time(funding_time_ist)} | Remaining: {time_remaining_str}")
            else:
                err = funding.get("error", "No funding info")
                log_error(f"[{idx}/{num_scanned}] {symbol} | [WARN] {err}")

    log_info("Funding fetch complete. Checking for opportunities...")

    # Main logic: Only one trade at a time
    for funding in funding_results:
        symbol = funding["symbol"]
        rate = funding.get("fundingRate")
        funding_timestamp = funding.get("nextFundingTime")
        price = funding.get("price")
        if rate is None or funding_timestamp is None or price is None:
            continue
        funding_time_utc = funding_timestamp / 1000
        funding_time_utc = datetime.fromtimestamp(funding_time_utc, tz=pytz.UTC)
        funding_time_ist = funding_time_utc.astimezone(IST)
        now_ist = datetime.now(IST)
        time_to_funding = (funding_time_ist - now_ist).total_seconds()

        if not trade_active and rate <= FUNDING_RATE_THRESHOLD and time_to_funding > 3000:  # >50 mins
            planned_entry_time = funding_time_ist - timedelta(minutes=50)
            time_to_entry = (planned_entry_time - now_ist).total_seconds()
            qty = truncate_qty(price, INITIAL_TRADE_USDT)
            if qty == 0 or time_to_entry < 0:
                log_info(f"Skipped {symbol}: qty=0 or entry time passed.")
                continue
            msg = (
                f"🚦 <b>Trade Planned!</b>\n"
                f"Coin: <b>{symbol}</b>\n"
                f"Price: {price}\n"
                f"Funding Rate: {rate:.4f}\n"
                f"Qty: <b>{qty}</b>\n"
                f"Entry at (IST): <b>{format_time(planned_entry_time)}</b>\n"
                f"Funding Round Ends (IST): <b>{format_time(funding_time_ist)}</b>\n"
                f"Time Remaining: <b>{format_time_remaining(time_to_funding)}</b>"
            )
            log_info(msg.replace('\n', ' '))
            send_telegram_to_all(msg)
            trade_active = True
            trade_planned = True
            current_trade = {
                "symbol": symbol,
                "planned_entry_time": planned_entry_time,
                "funding_end_time": funding_time_ist,
                "qty": qty,
                "price": price,
                "rate": rate
            }
            break

    # Send notification if no trade planned in this scan
    if not trade_planned:
        end_time = time.time()
        duration_str = f"{end_time - start_time:.2f} seconds"
        summary = (
            "<b>No trade found in this scan.</b>\n"
            f"Coins scanned: <b>{num_scanned}</b>\n"
            f"Time taken: <b>{duration_str}</b>"
        )
        log_info(summary.replace('\n', ' '))
        send_telegram_to_all(summary)

    # Execute planned trade
    if trade_active and current_trade:
        symbol = current_trade['symbol']
        planned_entry_time = current_trade['planned_entry_time']
        funding_end_time = current_trade['funding_end_time']
        qty = current_trade['qty']
        entry_price = current_trade['price']

        # Wait until 90 seconds before entry
        while True:
            now_ist = datetime.now(IST)
            seconds_to_entry = (planned_entry_time - now_ist).total_seconds()
            if seconds_to_entry <= 90:
                log_info(f"90 seconds before entry. Re-verifying signal and qty for {symbol}")
                funding = get_funding_data(symbol)
                rate = funding.get("fundingRate")
                price = funding.get("price")
                if rate is None or price is None:
                    log_error(f"Funding info missing on reverify for {symbol}. Trade canceled.")
                    send_telegram_to_all(f"❌ <b>Trade Canceled</b>\nReason: Funding info missing on reverify for {symbol}.")
                    trade_active = False
                    break
                qty_new = truncate_qty(price, INITIAL_TRADE_USDT)
                if qty_new == 0:
                    log_error(f"Price too high for $50 capital. Trade canceled.")
                    send_telegram_to_all(f"❌ <b>Trade Canceled</b>\nReason: Price too high for $50 capital.")
                    trade_active = False
                    break
                if rate > FUNDING_RATE_THRESHOLD:
                    log_info(f"Funding rate is no longer below threshold. Trade canceled.")
                    send_telegram_to_all(f"❌ <b>Trade Canceled</b>\nReason: Funding rate is no longer below threshold.")
                    trade_active = False
                    break
                qty = qty_new
                log_info(f"[REVERIFY] Qty: {qty}, Price: {price}, Funding Rate: {rate}")
                break
            else:
                time.sleep(10)

        # Wait for exact entry time
        while True and trade_active:
            now_ist = datetime.now(IST)
            seconds_to_entry = (planned_entry_time - now_ist).total_seconds()
            if seconds_to_entry <= 0:
                log_info(f"[ENTRY] Entering trade {symbol} at qty {qty} and price {price}")
                set_leverage(symbol, TRADE_LEVERAGE)
                tried_qty = qty
                order = None
                while tried_qty > 0:
                    order = place_market_long(symbol, tried_qty)
                    if order:
                        log_info(f"Trade executed: {order}")
                        break
                    else:
                        tried_qty = int(tried_qty * 0.95)
                        log_info(f"[RETRY] Retrying with qty {tried_qty}")
                if not order:
                    log_error(f"Unable to place trade for {symbol}. Trade canceled.")
                    send_telegram_to_all(f"❌ <b>Trade Canceled</b>\nReason: Unable to place trade for {symbol}.")
                    trade_active = False
                    break

                # Get entry price from order fill (if available)
                entry_price = float(order['avgFillPrice']) if 'avgFillPrice' in order and order['avgFillPrice'] else price
                send_telegram_to_all(
                    f"🚀 <b>Trade Executed</b>\nCoin: <b>{symbol}</b>\nQty: <b>{tried_qty}</b>\nEntry Price: {entry_price}\nLeverage: <b>{TRADE_LEVERAGE}x</b>\nTime: {format_time(now_ist)}"
                )
                # Place SL
                sl_order = place_stop_loss(symbol, entry_price, tried_qty)
                if sl_order:
                    send_telegram_to_all(
                        f"🛡️ <b>Stop Loss Placed</b>\nSymbol: <b>{symbol}</b>\nQty: <b>{tried_qty}</b>\nSL Price: <b>{round(entry_price * (1 - STOP_LOSS_PCT), 6)}</b>"
                    )
                else:
                    send_telegram_to_all(
                        f"⚠️ <b>Stop Loss Failed</b>\nSymbol: <b>{symbol}</b>\nQty: <b>{tried_qty}</b>"
                    )
                # Schedule exit
                exit_time = funding_end_time - timedelta(minutes=1)
                seconds_to_exit = (exit_time - now_ist).total_seconds()
                log_info(f"[EXIT PLANNED] Will exit at {format_time(exit_time)} (in {format_time_remaining(seconds_to_exit)})")
                time.sleep(max(0, seconds_to_exit))

                # Close position (if still open: i.e. if SL not hit)
                close_order = close_market_long(symbol, tried_qty)
                if close_order:
                    log_info(f"Position closed: {close_order}")
                    send_telegram_to_all(
                        f"🔒 <b>Trade Closed (Scheduled Exit)</b>\nCoin: <b>{symbol}</b>\nQty: <b>{tried_qty}</b>\nExit Time: {format_time(datetime.now(IST))}"
                    )
                else:
                    log_error(f"Failed to close position for {symbol}. Please check manually.")
                    send_telegram_to_all(
                        f"❌ <b>Trade Close Failed</b>\nCoin: <b>{symbol}</b>\nQty: <b>{tried_qty}</b>\nExit Time: {format_time(datetime.now(IST))}"
                    )
                # Report balance, next capital
                usdt_balance = get_futures_balance()
                next_capital = round(usdt_balance * NEXT_TRADE_CAPITAL_PCT, 2)
                send_telegram_to_all(
                    f"💰 <b>P&L Report</b>\nSymbol: <b>{symbol}</b>\nQty: <b>{tried_qty}</b>\nRemaining USDT: <b>{usdt_balance:.2f}</b>\nNext trade capital: <b>{next_capital:.2f}</b>"
                )
                trade_active = False
                break
            else:
                time.sleep(10)

    end_time = time.time()
    duration_str = f"{end_time - start_time:.2f} seconds"
    log_info(f"Scan finished. Coins scanned: {num_scanned}. Time taken: {duration_str}")

def sleep_until_next_half_hour():
    now_ist = datetime.now(IST)
    next_half_hour = now_ist.replace(minute=30, second=0, microsecond=0)
    if now_ist.minute >= 30:
        next_half_hour += timedelta(hours=1)
    sleep_seconds = (next_half_hour - now_ist).total_seconds()
    log_info(f"Sleeping for {sleep_seconds/60:.1f} minutes until next scan at {next_half_hour.strftime('%H:%M')} IST.")
    time.sleep(max(0, sleep_seconds))

def main():
    log_info("Starting Funding Rate Bot with IP whitelist verification.")
    verify_ip_whitelist()
    send_telegram_to_all(
        f"🚀 <b>FUNDING RATE BOT STARTED</b>\n"
        f"⚙️ Status: <b>Online</b>\n"
        f"📊 Threshold: <b>{FUNDING_RATE_THRESHOLD*100:.2f}%</b>\n"
        f"⚡ Leverage: <b>{TRADE_LEVERAGE}x</b>\n"
        f"🕐 Time: <b>{format_time(datetime.now(IST))} IST</b>"
    )
    while True:
        try:
            scan_opportunities()
            sleep_until_next_half_hour()
        except Exception as e:
            log_error(f"Critical error in main loop: {e}")
            send_telegram_to_all(
                f"🚨 <b>CRITICAL ERROR</b>\n🚫 Error: <b>{str(e)}</b>\n🔄 Action: <b>Restarting in 1 minute</b>\n🕐 Time: <b>{format_time(datetime.now(IST))} IST</b>"
            )
            time.sleep(60)

if __name__ == "__main__":
    main()