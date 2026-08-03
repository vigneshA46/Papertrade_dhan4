from find_instrument import FindInstrument

finder = FindInstrument()

ce = finder.get_option("NIFTY", 23500, "CE")
print(ce)

