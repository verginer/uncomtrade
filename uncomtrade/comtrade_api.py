import requests
import logging
import subprocess
import psutil

TIMEOUT = 5
MAX_TRY = 3


def get_commodity_data(country_code, commodity_code="TOTAL", year=2013, classification="S1"):
    """
    get_commodity_data returns the comtrade data in json format for the given parameters
    Args:
        country_code (int): code number as defined by comtrade
        commodity_code (str): commodity code as defined by comtrade
        year (int): 4 digit e.g. 2003

    Returns:

    """
    # global get_request

    parameters = {"max": 50000,
                  "type": "C",
                  "freq": "A",
                  "px": classification,
                  "ps": year,
                  "r": country_code,
                  "p": "all",
                  "rg": 1,
                  "cc": commodity_code,
                  "fmt": "json"
                  }
    trial = 0
    failed = True

    while failed:
        try:
            get_request = requests.get("http://comtrade.un.org/api/get?", params=parameters, timeout=TIMEOUT)
            failed = False
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as err:
            logging.warning("request timed out at country {country} at trial: {trial} for error: {err}".format(
                country=country_code,
                trial=trial,
                err=err))
            trial += 1
            if trial > MAX_TRY:
                logging.warning("! Giving up on request, country: {country}, year: {year} !".format(
                    country=country_code,
                    year=year)
                )
                return []
            else:
                continue

    if get_request.status_code == 200:
        try:
            # payload = json.loads(get_request.content.decode("utf-8"))["dataset"]
            payload = get_request.json()["dataset"]
            return payload
        except ValueError:
            logging.warning("country_code: {country}, skipped because of ValueError in json".format(
                            country=country_code))
            return "JSON Error"
    else:
        return get_request.status_code


def _kill_proc(name):
    for proc in psutil.process_iter():
        if proc.name() == name:
            proc.kill()


def swithch_vpn(on_off="off"):
    if on_off is "off":
        # pia_tray
        try:
            _kill_proc("launch_pia")
            _kill_proc("pia_tray")
            _kill_proc("openvpn")
            _kill_proc("pia_openvpn")
        except:
            pass
        return "off"
    elif on_off is "on":
        subprocess.Popen([r"/Applications/Private Internet Access.app/Contents/MacOS/launch_pia"])
        return "on"


