BANK NIFTY OPTION BUYING

The banknifty_option_buying strategy is an intraday option buying system built on Bank Nifty, where strike selection is done using the Bank Nifty index and execution is done in options. The ATM strike for both CE and PE is selected from the close of the first 1-minute Bank Nifty candle. For each option, the close of its first 1-minute candle is marked as a reference line. A buy entry is triggered when a candle closes above this marked line and the average price is above the line but below the candle close, with execution on the current LTP; additionally, an entry is also allowed if any tick moves more than +25 points above the marked line without obeying the average price rule. Exits occur if a candle closes below the marked line or if any tick moves 25 points below it.

Each trade uses a trailing stop-loss system where TSL starts at +40 points from entry, SL is placed 15 points below the TSL, and TSL activates only after price touches it; as the market moves, both TSL and SL trail upward by 10 points. Lot sizing is progressive and merged across CE and PE: the first trade of the day starts with 1 lot, and every new trade—regardless of CE or PE—increases the lot size by 1, freezing at a maximum of 5 lots. If a trade exits via TSL, the lot size resets back to 1 and then starts increasing again on subsequent trades. If a trade has its TSL activated and later exits at SL, the next entry on that option is allowed only after price moves back below the marked line and re-qualifies.

Universal exits apply where all positions are closed and trading stops once cumulative profit reaches 100 points, or when the time reaches 15:20, whichever comes first.

✅ SHEET 1 — BankNiftyLogger (DAY / SESSION LOGS)

This sheet stores session-level information, not individual trades.

📄 Columns Column Name Purpose Date Trading date Strategy banknifty_option_buying ATM Calculated ATM strike StartTime Engine start time EndTime Engine stop time TotalTrades Number of trades CETrades Number of CE trades PETrades Number of PE trades DayPNL Final PNL MaxProfit Max cumulative profit reached MaxDrawdown Worst drawdown ExitReason TARGET_HIT / TIME_EXIT / MANUAL Notes Optional remarks ✅ SHEET 2 — BankNiftyTrade (TRADE BOOK)

This stores every entry + exit.

📄 Columns Column Name Purpose Timestamp Entry/Exit timestamp Symbol BANKNIFTY OptionType CE / PE SecurityID Option security id Side BUY / SELL EntryPrice Entry price ExitPrice Exit price Lots Lots used Qty Lots × lot size PNL Trade pnl CumulativePNL Running pnl EntryReason CandleBreak / TickBreak ExitReason MarkedLine / TickSL / TSL / Time MarkedLine First candle close TSL Final tsl SL Final sl

nifty option buying 50 points no reentry

This Nifty Option Buying strategy operates entirely on 1-minute charts and uses the Nifty index for strike selection while executing trades in options. At market open, the close of the first 1-minute Nifty candle is taken as ATM and used as the strike for both CE and PE, which are traded independently. For each option, the close of its first 1-minute candle is marked as a reference line. A buy is triggered when a candle closes above this line, the average price is above the line, and the average price is below the candle close, upon which one lot is bought . The entry is taken in the next candle . The position is exited if any candle closes below the marked line, and re-entries are allowed when conditions match again, with the lot size increasing by one on every new trade independently for CE and PE. Universal exits apply where all open positions are closed and no further trades are taken if the combined CE+PE profit reaches 50 points or when the time hits 15:20.

RANGE BREAKOUT SELLING

This strategy begins by observing the NIFTY index on the 5-minute chart and marking the open and close of the 9:55–10:00 candle, where the higher of the open/close becomes the top line and the lower becomes the bottom line. The close of that candle is used as the ATM reference, from which option strikes are selected: CE = ATM − 400 and PE = ATM + 400. After strike selection, the index chart is switched to 1-minute resolution while keeping the same marked levels, and both the selected CE and PE are traded using 1-minute option candles. From 10:01 onward, trades are evaluated independently using the index: if any 1-minute index candle closes above the top line, and the candle’s average price ((O+H+L+C)/4) is above the top line and below the candle close, a PE sell signal is generated, and the actual PE entry is executed on the next candle at market price. The PE position is exited when any index candle closes back below the top line. Similarly, if any candle closes below the bottom line, and the candle’s average price is below the bottom line and above the candle close, a CE sell signal is generated, and the CE entry is executed on the next candle. The CE position is exited when any index candle closes back above the bottom line. Each option position starts with a fixed SL of −15 points and a trailing stop of −30 points from entry, but the SL and TSL become active only after price first moves beyond the TSL level, and as price continues in favor, both the SL and TSL trail forward in steps of 10 points. In addition, a day-level risk rule is enforced: and if total pnl reaches 38, all positions are closed and trading stops for the day.

VWAP STRATERGY PAPER TRADE

The VWAP strategy is an intraday options buying strategy focused on the NIFTY market. Strike selection is based on the current-month NIFTY futures chart on a 1-minute timeframe, where the close of the first 1-minute candle after market open is taken as ATM; from this, the CE strike is selected as 2 ITM and the PE strike as 2 ITM. After selecting the strikes, each option (CE and PE) is traded independently on its own 1-minute option chart using the VWAP indicator. From 9:16 onwards, if any candle closes above the VWAP line, a buy entry of 1 lot is taken on the next candle, and the position is held as long as price remains above VWAP. If any candle closes below the VWAP, the open position is exited on the same candle close. In addition to individual exits, a cumulative MTM risk rule is applied: on every candle, the combined MTM of all running trades is calculated, and if the total profit or loss reaches ±3000, all positions are force-exited immediately and no further trades are allowed for that trading day.

MCX CRUDEOIL STRATERGY

This is an intraday option-buying strategy on MCX Crude Oil, where the ATM strike for the day is selected using the close of the 3:15–3:30 PM candle of the current-month Crude Oil Futures on a 15-minute chart, and the same strike is traded for both CE and PE independently on 1-minute option charts. On each option chart, the 3:30 close price is marked and a +8-point buffer is drawn above it; a buy entry is triggered when a 1-minute candle closes above the buffer, the candle’s average price ((O+H+L+C)/4) is also above the buffer, and the average price remains below the candle close, with execution at the next candle open for one lot. A trade is exited if any candle closes below the marked 3:15 price, if a 50-point target is achieved—upon which all CE and PE positions are exited and no further trades are allowed for the day—or via a trailing stop-loss, which activates at +15 points from the entry, places an SL 10 points below the active TSL, and trails upward in 10-point increments as price advances. Re-entry is permitted only after the combined CE+PE drops below −25 points and the daily target has not been hit, requiring a reset candle close below the buffer followed by a fresh valid breakout above the buffer, while a daily cumulative pnl of −25 points or worse restricts trading strictly to this controlled re-entry logic.

DELTA AVERAGE PRICE

This is an intraday NIFTY option buying strategy (MIS) where strike selection is based on the NIFTY index, and trades are executed on high-delta options (Δ ≥ 0.85) for both CE and PE, each traded independently on 1-minute charts. The close of the first 1-minute candle on the option chart is marked as a reference line, and for every candle an average price is calculated using (𝑂+𝐻+𝐿+𝐶)/4 from option LTP values. A buy entry is taken on the next candle open in one lot when a candle closes above the marked line and its average price is below the candle close and the marked ine is above the marked line, indicating momentum strength. A normal exit condition is always active: if any candle closes below the marked line, the trade is exited immediately, and this exit rule operates independently of the trailing stop-loss logic.

Alongside the normal exit, the strategy uses a fixed cumulative target of 100 points and a dynamic trailing stop-loss (TSL). The initial TSL is placed at +30 points from the entry price, and trailing becomes active only when any candle touches this TSL level. Once activated, the stop-loss is set 10 points below the TSL. If price continues to move in favor and advances 10 points above the current TSL, both the TSL and SL are moved up by 10 points, progressively locking in profits. During the trailing phase, the trade exits only when a candle closes below the current SL. After a TSL-based exit or target hit, the next trade is always taken with 1 lot, and a fresh entry is allowed only after price first moves below the marked line and then again satisfies the original entry conditions, ensuring disciplined resets before re-entry.



NIFTY VWAP EMA5 OPTION BUYING 

This strategy is an intraday NIFTY option buying system that uses the NIFTY index for signal generation and fixed CE/PE option contracts for execution. At 09:19 AM, the CE and PE strikes whose premium is nearest to or above ₹250 are selected, and the same strikes are traded throughout the day. The strategy analyses 1-minute index candles using VWAP and EMA(5). From market open until 09:45 AM, the system waits for at least one EMA–VWAP crossover; however, if no crossover occurs before 09:45 AM, trading becomes enabled regardless. A bullish entry is triggered when an index candle closes above VWAP while EMA(5) remains above VWAP, resulting in a one-lot CE buy, whereas a bearish entry is triggered when an index candle closes below VWAP while EMA(5) remains below VWAP, resulting in a one-lot PE buy. Trade management is handled on the option premium chart itself, where each position uses a fixed stop loss of 25 points and a trailing profit mechanism starting at +35 points with trailing increments of 10 points. If a trade exits through the trailing mechanism, re-entry is allowed only after the index price touches VWAP again or a fresh EMA–VWAP crossover occurs, followed by a valid setup confirmation. Additionally, any active position is forcefully exited if the index price touches VWAP or if EMA crosses VWAP against the active trade direction. The system also enforces strict daily MTM risk management: all trades are immediately closed and trading is disabled for the remainder of the day once either a total profit of ₹5,000 or a total loss of ₹2,500 is reached.

google sheet logging

vwap option buying -- not done

nifty option buying 50 noreentry --

nifty option buying 50 reentry

nifty option buying 35 reentry

delta option buying

mcx crude oil stratergy

bank nifty option buying

range breakout selling





//on option tick function in range breakout selling

""" 
def on_option_candle(token, t, row):
    global ce_pos, pe_pos, pending_ce, pending_pe

    if stop_trading:
        return

    # ---- EXECUTION ----
    if token == CE_ID and pending_ce:
        price = row["open"]
        ce_pos = {
            "entry_time": t,
            "entry_price": price,
            "best": price,
            "sl": price - 15,
            "trail": price - 30,
            "active": False,
        }
        pending_ce = False
        print("✅ CE SELL @", price, t)

    if token == PE_ID and pending_pe:
        price = row["open"]
        pe_pos = {
            "entry_time": t,
            "entry_price": price,
            "best": price,
            "sl": price - 15,
            "trail": price - 30,
            "active": False,
        }
        pending_pe = False
        print("✅ PE SELL @", price, t)

    # ---- MANAGEMENT ----
    if token == CE_ID and ce_pos:
        manage_position("CE", row["close"], t)

    if token == PE_ID and pe_pos:
        manage_position("PE", row["close"], t)


 """
