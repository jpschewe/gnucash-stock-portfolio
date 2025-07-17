#!/usr/bin/env python3

import warnings
with warnings.catch_warnings():
    import re
    import sys
    import argparse
    import os
    import logging
    import gnucash 
    import logging
    import logging.config
    import json
    from pathlib import Path
    import subprocess
    import datetime
    import time
    from gnucash_patch import GncPrice
    import fractions
    import platformdirs
    import bisect
    
SCRIPT_DIR=Path(__file__).parent.absolute()

# Maximum number of times to retry getting a quote.
# This only applies to sources that have a delay listed.
# Each time the retry is done, the delay multiplier is increased.
MAX_RETRIES = 3

# quote source -> time
last_query = dict()

# minimum delay per quote source
# 5 API requests per minute is the limit to alphavantage
# That is once every 12 seconds.
delay = { 'alphavantage': datetime.timedelta(seconds=12) }


def get_logger():
    return logging.getLogger(__name__)


def setup_logging(
    default_path='logging.json',
    default_level=logging.INFO,
    env_key='LOG_CFG'
):
    """
    Setup logging configuration
    """
    try:
        path = Path(default_path)
        value = os.getenv(env_key, None)
        if value:
            path = Path(value)
        if path.exists():
            with open(path, 'r') as f:
                config = json.load(f)
            logging.config.dictConfig(config)
        else:
            logging.basicConfig(level=default_level)
    except:
        print(f"Error configuring logging, using default configuration with level {default_level}: {err=}, {type(err)=}")
        logging.basicConfig(level=default_level)


def determine_commodities_to_check(last_commodity_symbol: str|None, account):
    """
    Find all non-currency commodities with a non-zero balance.
    Sort the list by symbol.
    Reorder the list to start after last_commodity_symbol and continue through all commodities.
    
    Arguments:
      last_commodity_symbol: last commodity checked
    Returns:
      list of commodities to check in the order they should be checked
    """
    commodities_to_check = set()
    for acc in account.get_descendants():
        if acc.GetType() == gnucash.ACCT_TYPE_STOCK or acc.GetType() == gnucash.ACCT_TYPE_MUTUAL:
            if acc.GetBalance().to_double() > 0:
                commodity = acc.GetCommodity()
                namespace = commodity.get_namespace()
                if namespace != 'CURRENCY':
                    commodities_to_check.add(commodity)

    key_func = lambda c: c.get_nice_symbol()
    sorted_commodities = list(sorted(commodities_to_check, key=key_func))

    if last_commodity_symbol is None:
        return sorted_commodities
    else:
        # reorder list to check all commodities after the last symbol checked first
        split_idx = bisect.bisect(sorted_commodities, last_commodity_symbol, key=key_func)
        under = sorted_commodities[:split_idx]
        over = sorted_commodities[split_idx:]
        return over + under


def call_gnc_fq(symbol, source_name):
    get_logger().debug("Getting price symbol %s source %s", symbol, source_name)
        
    input_string = '({} "{}")'.format(source_name, symbol)
    get_logger().debug("Sending to process '%s'", input_string)
    process = subprocess.Popen(["gnc-fq-helper"], stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    (output, error) = process.communicate(input=input_string)
    output = output.rstrip()
    error = error.rstrip()
    get_logger().debug("output: '%s'", output)
    get_logger().debug("error: '%s'", error)

    if re.match(r'^(#f)', output) is not None:
        # failed to find price
        return None, None, None
    else:
        # parse output
        match = re.match(r'^\(\("\S+" \(symbol \. "\S+"\) \(gnc:time-no-zone \. "(?P<datetime>[^"]+)"\) \(last \. (#e)?(?P<value>\d+\.\d+)\) \(currency \. "(?P<currency>\S+)"\)\)\)', output)
        if match:
            return match.group("value"), match.group("currency"), match.group("datetime")
        else:
            get_logger().warning("No match on output '%s'", output)
            return None, None, None

        
def execute_delay(source_name, quote_delay, multiplier):
    global last_query

    quote_last_query = last_query.get(source_name, None)
    
    now = datetime.datetime.now()
    
    if quote_last_query is not None:
        diff = now - quote_last_query
        wait_time = quote_delay * multiplier
        if diff < wait_time:
            sleep_time = wait_time - diff
            get_logger().debug("Sleeping for %d seconds for %s", sleep_time.total_seconds(), source_name)
            time.sleep(sleep_time.total_seconds())

    # use time after quote is finished to ensure that we don't creep up on the API limit
    last_query[source_name] = datetime.datetime.now()


def get_current_price(use_flatpak, symbol, source_name):
    """
    Returns:
      value: str
      currency: str
      quote date: str
    """
    get_logger().debug("Getting price symbol %s source %s", symbol, source_name)
    
    if use_flatpak:
        gnucash_command = "flatpak run --command=gnucash-cli org.gnucash.GnuCash"
    else:
        gnucash_command = "gnucash-cli"
    
    output = subprocess.check_output(f"{gnucash_command} --quotes dump {source_name} {symbol}", shell=True)
    output = output.decode().rstrip()
    get_logger().debug("output: '%s'", output)


    date = None
    value = None
    currency = None
    for line in output.splitlines():
        if m := re.match(r'^\s*date:\s+(\S+)', line):
            date = m.group(1)
        elif m := re.match(r'^\s*currency:\s+(\S+)', line):
            currency = m.group(1)
        elif m := re.match(r'^\s*(?:last|nav|price):\s+(\S+)', line):
            value = m.group(1)

        if date and value and currency:
            return value, currency, date
        
    # failed to find price
    return None, None, None
    
    
def get_quote(use_flatpak, symbol, source_name):
    global delay
    global last_query
    global MAX_RETRIES

    get_logger().debug("Getting quote for %s in %s", symbol, source_name)
    
    attempt = 1
    quote_delay = delay.get(source_name, None)

    # <= so that we try at least MAX_RETRIES times since we start at 1
    while attempt <= MAX_RETRIES:
        if quote_delay is not None and attempt > 1:
            get_logger().debug("Executing delay attempt: %s", attempt)
            execute_delay(source_name, quote_delay, attempt)

        #value, currency, quote_datetime = call_gnc_fq(symbol, source_name)
        value, currency, quote_datetime = get_current_price(use_flatpak, symbol, source_name)
        if quote_delay is not None and value is None:
            # if we failed to get a quote and there is a delay for this source, try again
            attempt = attempt + 1
            continue
        else:
            get_logger().debug("Returning value: %s", value)
            return value, currency, quote_datetime

    get_logger().debug("Exhausted retries for %s with %s", symbol, source_name)
    return None, None, None
        
        
def convert_float_to_gnumeric(value):
    f = fractions.Fraction(value)
    
    return gnucash.GncNumeric(f.numerator, f.denominator)


def parse_datetime(s):
    if s is None:
        return datetime.datetime.now()

    # 2019-11-29
    dt = datetime.datetime.strptime(s, "%m/%d/%Y")
    return dt


def update_price(use_flatpak, book, commodity):
    """
    Returns:
      bool: success
    """
    source = commodity.get_quote_source()
    if source:
        source_name = gnucash.gnucash_core_c.gnc_quote_source_get_internal_name(source)
    else:
        source_name = None
    get_logger().debug("symbol: %s name: %s quote: %s source: %s", commodity.get_nice_symbol(), commodity.get_fullname(), commodity.get_quote_flag(), source_name)

    if source_name is not None:
        value, currency, quote_datetime = get_quote(use_flatpak, commodity.get_nice_symbol(), source_name)
        get_logger().debug("Got value: %s currency: %s datetime: %s", value, currency, quote_datetime)
        
        if value and currency:
            table = book.get_table()
            gnc_currency = table.lookup('ISO4217', currency)
            p = GncPrice(book)
            p.set_time64(parse_datetime(quote_datetime))
            p.set_commodity(commodity)
            p.set_currency(gnc_currency)
            gnumeric_value = convert_float_to_gnumeric(value)
            p.set_value(gnumeric_value)
            p.set_source(gnucash.gnucash_core_c.PRICE_SOURCE_FQ)
            book.get_price_db().add_price(p)
            return True

    get_logger().debug("Unable to fetch price for %s from %s", commodity.get_nice_symbol(), source_name)
    return False


def get_save_file() -> Path:
    """
    Returns
      path to where the data is saved, file may not exist
    """
    data_dir = Path(platformdirs.user_data_dir(appname='gnucash-quotes', appauthor='jpschewe', ensure_exists=True))
    get_logger().debug("Data dir %s", data_dir)
    
    return data_dir / 'state.json'


def save_state(accounts_filename: Path, commodity_symbol: str):
    save_data = get_state()
    save_data[str(accounts_filename.absolute())] = commodity_symbol
    
    with open(get_save_file(), 'w') as f:
        json.dump(save_data, f)


def get_state() -> dict[str, str]:
    """
    Get the saved state.

    Returns:
      dictionary of save filename to last commodity symbol fetched
    """
    save_file = get_save_file()
    if not save_file.exists():
        return dict()
    
    with open(save_file, "r") as f:
        save_data = json.load(f)
        return save_data


def get_source_name(commodity) -> str|None:
    source = commodity.get_quote_source()
    if source:
        return gnucash.gnucash_core_c.gnc_quote_source_get_internal_name(source)
    else:
        return None
        

def update_prices(use_flatpak: bool, book, commodities_to_check) -> str|None:
    """
    Update prices for the specified commodities.
    Commodities up to the last symbol checked will be skipped.
    
    Arguments:
      use_flatpak: if true, call gnucash-cli through flatpak
      book: the gnucash book
      commodities_to_check: the commodities to check in sorted order from first to check to last to check
    Returns:
      the last commodity successfully fetched or None
    """
    get_logger().debug("commodities to check: %d %s", len(commodities_to_check), [c.get_nice_symbol() for c in commodities_to_check])

    # the last commodity successfully checked
    prev_commodity = None
    for commodity in commodities_to_check:
        commodity_symbol = commodity.get_nice_symbol()
        get_logger().debug("Checking commodity %s", commodity_symbol)

        source_name = get_source_name(commodity)
        if source_name is None:
            get_logger().debug("Skipping %s with no source", commodity_symbol)
            continue
            
        if not update_price(use_flatpak, book, commodity):
            get_logger().debug("update_price failed for %s", commodity_symbol)
            break

        prev_commodity = commodity


    if prev_commodity is not None:
        return prev_commodity.get_nice_symbol()
    else:
        return None
        

def main_method(args):
    accounts_file = Path(args.filename)
    if not accounts_file.exists():
        get_logger().error("%s doesn't exist", args.filename)
        return 1

    lockfile = Path(args.filename + ".LCK")
    if lockfile.exists():
        get_logger().error("Lockfile exists, cannot proceed")
        return 1

    saved_state = get_state()
    get_logger().debug("Found saved state: %s", saved_state)
    
    accounts_filename = str(accounts_file.absolute())
    last_commodity_symbol = saved_state.get(accounts_filename)
    get_logger().debug("Found last_commodity_symbol: %s", last_commodity_symbol)
    
    session = gnucash.Session(accounts_filename)
    try:
        book = session.book
        table = book.get_table()
        pricedb = book.get_price_db()
        currency_code = 'USD'
        currency = table.lookup('ISO4217', currency_code)
        account = book.get_root_account()

        commodities_to_check = determine_commodities_to_check(last_commodity_symbol, account)

        if len(commodities_to_check) > 0:
            last_commodity_fetched = update_prices(args.flatpak, book, commodities_to_check)

            if last_commodity_fetched is None:
                get_logger().error("Unable to get any quotes")
                result = False
            else:
                save_state(accounts_file, last_commodity_fetched)
                result = True
            session.save()
        else:
            result = True
    finally:
        session.end()
        session.destroy()

    if result:
        return 0
    else:
        return 1

    
def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]

    class ArgumentParserWithDefaults(argparse.ArgumentParser):
        """
        From https://stackoverflow.com/questions/12151306/argparse-way-to-include-default-values-in-help
        """
        def add_argument(self, *args, help=None, default=None, **kwargs):
            if help is not None:
                kwargs['help'] = help
            if default is not None and args[0] != '-h':
                kwargs['default'] = default
                if help is not None:
                    kwargs['help'] += ' (default: {})'.format(default)
            super().add_argument(*args, **kwargs)
        
    parser = ArgumentParserWithDefaults(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-l", "--logconfig", dest="logconfig", help="logging configuration (default: logging.json)", default='logging.json')
    parser.add_argument("--debug", dest="debug", help="Enable interactive debugger on error", action='store_true')
    parser.add_argument("-f", "--file", dest="filename", help="file to read (required)", required=True)
    parser.add_argument("--flatpak", dest="flatpak", action='store_true', help="Set when using the flatpak installation of gnucash")

    args = parser.parse_args(argv)

    setup_logging(default_path=args.logconfig)
    if 'multiprocessing' in sys.modules:
        # requires the multiprocessing-logging module - see https://github.com/jruere/multiprocessing-logging
        import multiprocessing_logging
        multiprocessing_logging.install_mp_handler()

    if args.debug:
        import pdb, traceback
        try:
            return main_method(args)
        except:
            extype, value, tb = sys.exc_info()
            traceback.print_exc()
            pdb.post_mortem(tb)    
    else:
        return main_method(args)


if __name__ == "__main__":
    sys.exit(main())
    
