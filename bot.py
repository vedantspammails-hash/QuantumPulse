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
from binance.enums import ORDER_TYPE_MARKET, SIDE_BUY, SIDE_SELL
from binance.exceptions import BinanceAPIException
import math

# --- Load environment variables ---
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_IDS = os.getenv("TELEGRAM_CHAT_ID", "").replace(',', ' ').split()
BINANCE_API_KEY = os.getenv("API_KEY")
BINANCE_API_SECRET = os.getenv("API_SECRET")
IST = pytz.timezone("Asia/Kolkata")

FUNDING_RATE_THRESHOLD = -0.005
INITIAL_TRADE_USDT = 50
NEXT_TRADE_CAPITAL_PCT = 0.99
TRADE_LEVERAGE = 1
STOP_LOSS_PCT = 0.10

trade_count = 0  # Global variable to track number of trades executed

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

def get_public_ip():
    try:
        response = requests.get("https://api.ipify.org?format=json", timeout=10)
        response.raise_for_status()
        ip = response.json().get("ip")
        log_info(f"Detected Public IP: {ip}")
        send_telegram_to_all(
            f"🔔 <b>Railway Public IP</b>\nDetected IP: <b>{ip}</b>\n"
            "If you see API whitelist errors, please add this IP to Binance API key whitelist and restart the bot."
        )
        return ip
    except Exception as e:
        log_error(f"Failed to fetch public IP: {e}")
        send_telegram_to_all(f"❌ <b>Error fetching public IP</b>\nError: {e}")
        return None

def verify_binance_api():
    try:
        client = Client(BINANCE_API_KEY, BINANCE_API_SECRET, testnet=False)
        log_info("Binance client initialized.")
        acc_status = client.futures_account()
        log_info(f"Binance futures account loaded: canTrade={acc_status['canTrade']}, updateTime={acc_status['updateTime']}")
        send_telegram_to_all(
            f"✅ <b>Binance API Verified</b>\n"
            f"Futures trading enabled: <b>{acc_status['canTrade']}</b>\n"
            f"API UpdateTime: <b>{acc_status['updateTime']}</b>"
        )
        return client
    except BinanceAPIException as e:
        log_error(f"BinanceAPIException: {e}")
        send_telegram_to_all(
            f"❌ <b>Binance API Error</b>\nError: <b>{e.message}</b>\n"
            "Check API key, secret, permissions, and IP whitelist."
        )
        return None
    except Exception as e:
        log_error(f"General Binance error: {e}")
        send_telegram_to_all(
            f"❌ <b>Binance General Error</b>\nError: <b>{e}</b>\n"
            "Check API key, secret, permissions, and IP whitelist."
        )
        return None

def verify_bot_startup():
    log_info("Bot startup verification initiated.")
    send_telegram_to_all("🚦 <b>Bot Startup</b>\nVerifying Railway IP and Binance API status...")
    ip = get_public_ip()
    if not ip:
        log_error("Aborting: Could not get public IP for whitelisting.")
        exit(1)
    client = verify_binance_api()
    if not client:
        log_error("Aborting: Binance API verification failed.")
        exit(1)
    log_info("All startup checks passed.")
    send_telegram_to_all("✅ <b>Bot Startup Verification Passed</b>\nBot is online and ready for scanning.")
    return client

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
            "nextFundingTime": int(data.get("nextFundingTime", 0)),
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

def get_futures_balance(client):
    try:
        balance = client.futures_account_balance()
        usdt_balances = [float(b['balance']) for b in balance if b['asset'] == 'USDT']
        bal = usdt_balances[0] if usdt_balances else 0.0
        log_info(f"Current futures USDT balance: {bal}")
        return bal
    except Exception as e:
        log_error(f"Cannot fetch futures balance: {e}")
        return 0.0

def set_leverage(client, symbol, leverage):
    try:
        client.futures_change_leverage(symbol=symbol, leverage=leverage)
        log_info(f"Set leverage {leverage}x for {symbol}")
    except BinanceAPIException as e:
        log_error(f"Could not set leverage: {e}")

def place_market_long(client, symbol, qty):
    log_info(f"Attempting market BUY for {symbol} qty: {qty}")
    try:
        order = client.futures_create_order(
            symbol=symbol,
            side=SIDE_BUY,
            type=ORDER_TYPE_MARKET,
            quantity=qty
        )
        log_info(f"Market BUY successful for {symbol}, qty: {qty}, order: {order}")
        return order
    except BinanceAPIException as e:
        log_error(f"Order error: {e}")
        return None

def get_actual_entry_price(client, symbol, order, qty):
    if 'avgFillPrice' in order and float(order['avgFillPrice']) > 0:
        return float(order['avgFillPrice'])
    try:
        trades = client.futures_account_trades(symbol=symbol)
        for trade in reversed(trades):
            if trade['side'] == 'BUY' and abs(float(trade['qty']) - qty) < 1e-6:
                return float(trade['price'])
    except Exception as e:
        log_error(f"Could not fetch fills for {symbol}: {e}")
    return None

# New helper functions to handle quantity and price precisions:

def get_quantity_precision(client, symbol):
    try:
        exchange_info = client.futures_exchange_info()
        for s in exchange_info['symbols']:
            if s['symbol'] == symbol:
                for f in s['filters']:
                    if f['filterType'] == 'LOT_SIZE':
                        step_size = float(f['stepSize'])
                        return int(round(-math.log10(step_size), 0))
    except Exception as e:
        log_error(f"Error fetching quantity precision for {symbol}: {e}")
    return 3  # Default precision if error

def floor_to_precision(value, precision):
    factor = 10 ** precision
    return math.floor(value * factor) / factor

def truncate_qty(client, symbol, price, capital):
    if price <= 0:
        log_error(f"Price <= 0 for qty calc, returning 0")
        return 0
    precision = get_quantity_precision(client, symbol)
    raw_qty = capital / price
    qty = floor_to_precision(raw_qty, precision)
    log_info(f"Calculated qty: {qty} (precision {precision}) for capital: {capital} and price: {price}")
    return qty if qty > 0 else 0

def place_stop_loss(client, symbol, entry_price, qty):
    precision = get_quantity_precision(client, symbol)
    stop_price = round(entry_price * (1 - STOP_LOSS_PCT), 6)
    stop_price = floor_to_precision(stop_price, 6)
    qty = floor_to_precision(qty, precision)
    log_info(f"Placing stop loss at {stop_price} ({STOP_LOSS_PCT*100}% below entry price), qty: {qty}")
    try:
        order = client.futures_create_order(
            symbol=symbol,
            side=SIDE_SELL,
            type="STOP_MARKET",  # Use string for compatibility
            stopPrice=stop_price,
            quantity=qty,
            reduceOnly=True
        )
        log_info(f"Stop loss order placed at {stop_price}, order: {order}")
        return order
    except BinanceAPIException as e:
        log_error(f"Failed to place stop loss: {e}")
        return None

def cancel_open_stop_orders(client, symbol):
    cancelled_any = False
    try:
        open_orders = client.futures_get_open_orders(symbol=symbol)
        for order in open_orders:
            if order['type'] == "STOP_MARKET":
                client.futures_cancel_order(symbol=symbol, orderId=order['orderId'])
                log_info(f"Cancelled STOP_MARKET order {order['orderId']} for {symbol}")
                send_telegram_to_all(f"✅ Cancelled STOP_MARKET order <b>{order['orderId']}</b> for <b>{symbol}</b> after scheduled exit.")
                cancelled_any = True
        if not cancelled_any:
            log_info(f"No STOP_MARKET orders to cancel for {symbol}")
            send_telegram_to_all(f"ℹ️ No STOP_MARKET orders to cancel for <b>{symbol}</b> after scheduled exit.")
    except Exception as e:
        log_error(f"Failed to cancel open stop orders for {symbol}: {e}")
        send_telegram_to_all(f"❌ Error cancelling STOP_MARKET orders for <b>{symbol}</b> after scheduled exit.\nError: <b>{e}</b>")

def close_market_long(client, symbol, qty):
    log_info(f"Attempting market SELL (close) for {symbol} qty: {qty}")
    try:
        order = client.futures_create_order(
            symbol=symbol,
            side=SIDE_SELL,
            type=ORDER_TYPE_MARKET,
            quantity=qty,
            reduceOnly=True
        )
        log_info(f"Market SELL successful for {symbol}, qty: {qty}, order: {order}")
        return order
    except BinanceAPIException as e:
        log_error(f"Order error: {e}")
        return None

def scan_opportunities(client):
    global trade_count

    log_info("=== [SCAN STARTED] Scanning funding rate opportunities on Binance USDT-margined perpetual futures ===")
    start_time = time.time()
    symbols = get_binance_usdt_perpetual_symbols()
    num_scanned = len(symbols)
    log_info(f"Total pairs scanned: {num_scanned}")

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

    end_scan_time = time.time()
    scan_duration = end_scan_time - start_time
    log_info(f"Funding fetch for all {num_scanned} symbols complete. Scan duration: {scan_duration:.2f}s")

    shortlisted_signals = []
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
        time_to_funding = (funding_time_ist - now_ist).total_seconds() - scan_duration
        if rate < FUNDING_RATE_THRESHOLD and 0 < time_to_funding < 3600:
            shortlisted_signals.append({
                "symbol": symbol,
                "fundingRate": rate,
                "funding_time_ist": funding_time_ist,
                "time_to_funding": time_to_funding,
                "price": price
            })

    shortlisted_signals.sort(key=lambda x: x['time_to_funding'])
    summary_msg = (
        f"🔍 <b>Scan Summary</b>\n"
        f"Total Symbols Scanned: <b>{num_scanned}</b>\n"
        f"Time Taken: <b>{scan_duration:.2f} seconds</b>\n"
        f"Signals Passing Criteria: <b>{len(shortlisted_signals)}</b>"
    )
    send_telegram_to_all(summary_msg)
    log_info(summary_msg.replace('\n', ' '))

    if not shortlisted_signals:
        send_telegram_to_all("⛔ <b>No qualifying trade found in this scan.</b>")
        log_info("No shortlisted signals. Scan complete.")
        return

    for idx, signal in enumerate(shortlisted_signals):
        symbol = signal['symbol']
        rate = signal['fundingRate']
        funding_time_ist = signal['funding_time_ist']
        time_to_funding = signal['time_to_funding']
        price = signal['price']

        planned_entry_time = funding_time_ist - timedelta(minutes=50)
        time_to_entry = (planned_entry_time - datetime.now(IST)).total_seconds()

        # Determine capital to use
        if trade_count == 0:
            capital_to_use = INITIAL_TRADE_USDT
        else:
            usdt_balance = get_futures_balance(client)
            capital_to_use = usdt_balance * NEXT_TRADE_CAPITAL_PCT
        
        qty = truncate_qty(client, symbol, price, capital_to_use)

        if qty == 0 or time_to_entry < 0:
            log_info(f"Skipped {symbol}: qty=0 or entry time passed.")
            continue

        plan_msg = (
            f"🗂️ <b>Trade Plan #{idx+1}</b>\n"
            f"Symbol: <b>{symbol}</b>\n"
            f"Price: <b>{price}</b>\n"
            f"Funding Rate: <b>{rate:.4f}</b>\n"
            f"Qty: <b>{qty}</b>\n"
            f"Capital Used: <b>{capital_to_use:.2f} USDT</b>\n"
            f"Planned Entry (IST): <b>{format_time(planned_entry_time)}</b>\n"
            f"Funding Round Ends (IST): <b>{format_time(funding_time_ist)}</b>\n"
            f"Funding Time Left: <b>{format_time_remaining(time_to_funding)}</b>"
        )
        send_telegram_to_all(plan_msg)
        log_info(plan_msg.replace('\n', ' '))

        while True:
            now_ist = datetime.now(IST)
            seconds_to_entry = (planned_entry_time - now_ist).total_seconds()
            if seconds_to_entry <= 90:
                log_info(f"90 seconds before entry for {symbol}. Re-verifying signal and qty.")
                prior_msg = (
                    f"⏳ <b>90 Seconds Prior Signal Check</b>\n"
                    f"Symbol: <b>{symbol}</b>\n"
                    f"Re-checking funding rate, price and eligibility before entry."
                )
                send_telegram_to_all(prior_msg)
                funding = get_funding_data(symbol)
                rate_check = funding.get("fundingRate")
                price_check = funding.get("price")
                if rate_check is None or price_check is None:
                    log_error(f"Funding info missing on reverify for {symbol}. Will try next signal if available.")
                    send_telegram_to_all(f"❌ <b>Trade Canceled</b>\nReason: Funding info missing on reverify for <b>{symbol}</b>.\nTrying next signal.")
                    break
                if trade_count == 0:
                    capital_to_use = INITIAL_TRADE_USDT
                else:
                    usdt_balance = get_futures_balance(client)
                    capital_to_use = usdt_balance * NEXT_TRADE_CAPITAL_PCT
                qty_new = truncate_qty(client, symbol, price_check, capital_to_use)
                if qty_new == 0:
                    log_error(f"Price too high for capital. Will try next signal.")
                    send_telegram_to_all(f"❌ <b>Trade Canceled</b>\nReason: Price too high for capital for <b>{symbol}</b>.\nTrying next signal.")
                    break
                if rate_check > FUNDING_RATE_THRESHOLD:
                    log_info(f"Funding rate is no longer below threshold. Will try next signal.")
                    send_telegram_to_all(f"❌ <b>Trade Canceled</b>\nReason: Funding rate is no longer below threshold for <b>{symbol}</b>.\nTrying next signal.")
                    break
                qty = qty_new
                price = price_check
                log_info(f"[REVERIFY PASSED] Qty: {qty}, Price: {price}, Funding Rate: {rate_check:.4f}")

                while True:
                    now_ist = datetime.now(IST)
                    seconds_to_entry = (planned_entry_time - now_ist).total_seconds()
                    if seconds_to_entry <= 0:
                        set_leverage(client, symbol, TRADE_LEVERAGE)
                        tried_qty = qty
                        order = None
                        while tried_qty > 0:
                            order = place_market_long(client, symbol, tried_qty)
                            if order:
                                log_info(f"Trade executed: {order}")
                                break
                            else:
                                tried_qty = floor_to_precision(tried_qty * 0.95, get_quantity_precision(client, symbol))
                                if tried_qty <= 0:
                                    break
                                log_info(f"[RETRY] Retrying with qty {tried_qty}")
                        if not order:
                            log_error(f"Unable to place trade for {symbol}. Will try next signal if available.")
                            send_telegram_to_all(
                                f"❌ <b>Trade Canceled</b>\nReason: Unable to place trade for <b>{symbol}</b> even after retries. Trying next signal."
                            )
                            break

                        actual_entry_price = get_actual_entry_price(client, symbol, order, tried_qty)
                        if actual_entry_price is None:
                            actual_entry_price = price
                            log_error(f"Could not determine actual fill price for {symbol}, reporting mark price.")

                        entry_funding_data = get_funding_data(symbol)
                        entry_funding_rate = entry_funding_data['fundingRate'] if entry_funding_data else None
                        entry_funding_rate_str = f"{entry_funding_rate:.4f}" if entry_funding_rate is not None else "N/A"
                        send_telegram_to_all(
                            f"🚀 <b>Trade Entry</b>\n"
                            f"Symbol: <b>{symbol}</b>\n"
                            f"Qty: <b>{tried_qty}</b>\n"
                            f"Entry Price (Mark): <b>{price}</b>\n"
                            f"Actual Entry Price (fill): <b>{actual_entry_price}</b>\n"
                            f"Funding Rate at Entry: <b>{entry_funding_rate_str}</b>\n"
                            f"Leverage: <b>{TRADE_LEVERAGE}x</b>\n"
                            f"Order ID: <b>{order.get('orderId')}</b>\n"
                            f"Entry Time (IST): <b>{format_time(now_ist)}</b>"
                        )

                        sl_order = place_stop_loss(client, symbol, actual_entry_price, tried_qty)
                        if sl_order:
                            send_telegram_to_all(
                                f"🛡️ <b>Stop Loss Placed</b>\nSymbol: <b>{symbol}</b>\nQty: <b>{tried_qty}</b>\nSL Price: <b>{round(actual_entry_price * (1 - STOP_LOSS_PCT), 6)}</b>\nOrder ID: <b>{sl_order.get('orderId')}</b>"
                            )
                        else:
                            send_telegram_to_all(
                                f"⚠️ <b>Stop Loss Failed</b>\nSymbol: <b>{symbol}</b>\nQty: <b>{tried_qty}</b>"
                            )
                        exit_time = funding_time_ist - timedelta(minutes=1)
                        seconds_to_exit = (exit_time - now_ist).total_seconds()
                        log_info(f"[EXIT PLANNED] Will exit at {format_time(exit_time)} (in {format_time_remaining(seconds_to_exit)})")
                        send_telegram_to_all(
                            f"⏳ <b>Exit Scheduled</b>\nSymbol: <b>{symbol}</b>\nExit Time: <b>{format_time(exit_time)}</b> (1 min before funding round ends)"
                        )
                        time.sleep(max(0, seconds_to_exit))

                        exit_funding_data = get_funding_data(symbol)
                        exit_funding_rate = exit_funding_data['fundingRate'] if exit_funding_data else None
                        exit_funding_rate_str = f"{exit_funding_rate:.4f}" if exit_funding_rate is not None else "N/A"

                        close_order = close_market_long(client, symbol, tried_qty)
                        now_exit = datetime.now(IST)
                        if close_order:
                            log_info(f"Position closed: {close_order}")
                            send_telegram_to_all(
                                f"🔒 <b>Trade Closed (Scheduled Exit)</b>\nCoin: <b>{symbol}</b>\nQty: <b>{tried_qty}</b>\nExit Time: <b>{format_time(now_exit)}</b>\nOrder ID: <b>{close_order.get('orderId')}</b>\nFunding Rate at Exit: <b>{exit_funding_rate_str}</b>"
                            )
                        else:
                            log_error(f"Failed to close position for {symbol}. Please check manually.")
                            send_telegram_to_all(
                                f"❌ <b>Trade Close Failed</b>\nCoin: <b>{symbol}</b>\nQty: <b>{tried_qty}</b>\nExit Time: <b>{format_time(now_exit)}</b>\nFunding Rate at Exit: <b>{exit_funding_rate_str}</b>"
                            )

                        # Cancel any open stop loss orders for this symbol
                        cancel_open_stop_orders(client, symbol)

                        usdt_balance = get_futures_balance(client)
                        trade_count += 1  # Increment trade count after a successful trade
                        next_capital = round(usdt_balance * NEXT_TRADE_CAPITAL_PCT, 2)
                        pnl_msg = (
                            f"💰 <b>P&L & Balance Report</b>\n"
                            f"Symbol: <b>{symbol}</b>\n"
                            f"Qty: <b>{tried_qty}</b>\n"
                            f"Entry Price: <b>{actual_entry_price}</b>\n"
                            f"Exit Time: <b>{format_time(now_exit)}</b>\n"
                            f"Funding Rate at Entry: <b>{entry_funding_rate_str}</b>\n"
                            f"Funding Rate at Exit: <b>{exit_funding_rate_str}</b>\n"
                            f"Current Futures USDT Balance: <b>{usdt_balance:.2f}</b>\n"
                            f"Next trade capital: <b>{next_capital:.2f}</b>"
                        )
                        send_telegram_to_all(pnl_msg)
                        log_info(f"[TRADE COMPLETE] {symbol} qty={tried_qty} entry={actual_entry_price} exitTime={format_time(now_exit)}")
                        return
                    else:
                        time.sleep(10)
            else:
                time.sleep(10)
    send_telegram_to_all("❌ <b>All shortlisted signals failed 90-sec check or execution. No trade taken this scan.</b>")
    log_info("All shortlisted signals failed. Scan complete.")

def sleep_until_next_half_hour():
    now_ist = datetime.now(IST)
    next_half_hour = now_ist.replace(minute=30, second=0, microsecond=0)
    if now_ist.minute >= 30:
        next_half_hour += timedelta(hours=1)
    sleep_seconds = (next_half_hour - now_ist).total_seconds()
    log_info(f"Sleeping for {sleep_seconds/60:.1f} minutes until next scan at {next_half_hour.strftime('%H:%M')} IST.")
    time.sleep(max(0, sleep_seconds))

def main():
    log_info("===== Funding Rate Bot Starting =====")
    client = verify_bot_startup()
    usdt_balance = get_futures_balance(client)
    start_msg = (
        f"🚀 <b>FUNDING RATE BOT STARTED</b>\n"
        f"⚙️ Status: <b>Online</b>\n"
        f"📊 Threshold: <b>{FUNDING_RATE_THRESHOLD*100:.2f}%</b>\n"
        f"⚡ Leverage: <b>{TRADE_LEVERAGE}x</b>\n"
        f"💰 Initial USDT Balance: <b>{usdt_balance:.2f}</b>\n"
        f"🕐 Start Time: <b>{format_time(datetime.now(IST))} IST</b>\n"
        f"📡 Scans: Immediate first, then every next HH:30 (e.g. 10:30, 11:30 ...)"
    )
    send_telegram_to_all(start_msg)
    log_info(start_msg.replace('\n', ' '))
    first_scan = True
    while True:
        if not first_scan:
            sleep_until_next_half_hour()
        scan_opportunities(client)
        first_scan = False

if __name__ == "__main__":
    main()
