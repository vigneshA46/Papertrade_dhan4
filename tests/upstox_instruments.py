import requests




def get_upstox_option(
        access_token,
        symbol="NIFTY",
        option_type="CE",      # CE / PE
        mode="ATM",            # ATM / OFFSET / STRIKE
        offset=0,              # used if OFFSET
        strike=None,           # used if STRIKE
        expiry="current_week"
     ):
        try:
            url = "https://api.upstox.com/v2/instruments/search"

            headers = {
                "Accept": "application/json",
                "Authorization": f"Bearer {access_token}"
            }

            params = {
                "instrument_types": option_type,
                "expiry": expiry,
                "page_number": 1,
                "records": 1
            }

            # 🔥 Mode handling
            if mode == "ATM":
                params["query"] = symbol
                params["atm_offset"] = 0

            elif mode == "OFFSET":
                params["query"] = symbol
                params["atm_offset"] = offset

            elif mode == "STRIKE":
                if strike is None:
                    return {"status": "error", "message": "strike required"}
                params["query"] = f"{symbol} {strike}"

            else:
                return {"status": "error", "message": "invalid mode"}

            response = requests.get(url, headers=headers, params=params)
            data = response.json()

            if data["status"] != "success" or not data["data"]:
                return {"status": "error", "message": "No instrument found"}

            inst = data["data"][0]

            return {
                "status": "success",
                "instrument_key": inst["instrument_key"],
                "lot_size": inst["lot_size"],
                "trading_symbol": inst["trading_symbol"],
                "strike": inst["strike_price"],
                "expiry": inst["expiry"]
            }

        except Exception as e:
            return {"status": "error", "message": str(e)}



inst = get_upstox_option(
            access_token="eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIzWUNDNVMiLCJqdGkiOiI2OWQ5ZGZhYjIwMTk3ZjE2ZDM2ZmNhMmUiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlzRXh0ZW5kZWQiOnRydWUsImlhdCI6MTc3NTg4NjI1MSwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxODA3NDgwODAwfQ.D0loZfAnrwGYyxXoAiI5wnZAWX2lbonj1knxZeyC07Y",
            symbol="NIFTY",
            option_type="CE",
            mode="STRIKE",
            strike=24000
        )

            
        
print(inst)