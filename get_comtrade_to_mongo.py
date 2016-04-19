import pymongo
from uncomtrade import comtrade_api
from uncomtrade.notification import notify
import time
import logging

logging.basicConfig(filename='com_trade.log', level=logging.DEBUG)

INTERVAL_BETWEEN_REQUESTS = 2
MINUTE = 60
MINUTES_BETWEEN_409_REQUESTS = 3

COMMODITY = "TOTAL"

client = pymongo.MongoClient()
COLLECTION = client.comtrade.total

country_dict = list(client.comtrade.country_codes.find({}, {"id": 1, "_id": 0}))
country_codes = [code["id"] for code in country_dict if "all" not in code["id"]]

VPN_STATUS = "on"  # !!!!! TURN THE FREAKING VPN OFF !!!!!!


LOGO = r"""
 #     # #     #     #####
 #     # ##    #    #     #  ####  #    # ##### #####    ##   #####  ######
 #     # # #   #    #       #    # ##  ##   #   #    #  #  #  #    # #
 #     # #  #  #    #       #    # # ## #   #   #    # #    # #    # #####
 #     # #   # #    #       #    # #    #   #   #####  ###### #    # #
 #     # #    ##    #     # #    # #    #   #   #   #  #    # #    # #
  #####  #     #     #####   ####  #    #   #   #    # #    # #####  ######

                                            (c) Luca Verginer
"""

print(LOGO)


def is_in_db(country_cd, com_code, year_check):
    return True if COLLECTION.find_one({"rtCode": int(country_cd), "cmdCode": com_code, "yr": year_check}) else False


for year in range(1975, 2015):
    logging.info("-*"*10)
    logging.info(msg="Starting Year: {year}".format(year=year))
    logging.info("-*"*10)
    i = len(country_codes)-1

    # i = START_POSITION
    while i >= 0:
        country = country_codes[i]
        if is_in_db(country, COMMODITY, year):
            logging.debug("already in DB, country: {country}, year: {year} at position, {pos}".format(
                country=country,
                year=year,
                pos=i)
            )
            i -= 1
            continue

        time.sleep(INTERVAL_BETWEEN_REQUESTS)

        json_data = comtrade_api.get_commodity_data(country, COMMODITY, year)

        if type(json_data) is list and json_data:
            try:
                COLLECTION.insert_many(json_data)
                logging.info(msg="inserted country: {country} at position {position} using insert_many".format(
                    country=country,
                    position=i)
                )
                i -= 1
            except pymongo.errors.BulkWriteError:
                for element in json_data:
                    COLLECTION.insert_one(element)
                logging.debug(msg="inserted country: {country} at position {position} using insert_one".format(
                    country=country,
                    position=i)
                )
                i -= 1
                continue
        elif type(json_data) is list and not json_data:
            res = COLLECTION.insert_one({"rtCode": int(country), "cmdCode": COMMODITY, "yr": year, "pfCode": "S1"})
            logging.debug("inserted empty entry in MongoDB at (Object_ID) {id} at position {pos}".format(id=str(res.inserted_id), pos=i))
            i -= 1
        elif json_data == 409:

            message = "409 code received at: {country} at position {position},sleeping for {sleep}".format(
                country=country,
                sleep=MINUTES_BETWEEN_409_REQUESTS,
                position=i)
            logging.warning(msg=message)

            notify(title="UN Comtrade Quota Limit reached",
                   subtitle="Please change proxy",
                   message=message,
                   sound='bell')

            if VPN_STATUS is "on":
                VPN_STATUS = comtrade_api.swithch_vpn("off")
            else:
                VPN_STATUS = comtrade_api.swithch_vpn("on")

            for minute in range(MINUTES_BETWEEN_409_REQUESTS):
                logging.debug("{minute} minute(s) of {max_min} passed.".format(
                    minute=minute,
                    max_min=MINUTES_BETWEEN_409_REQUESTS))
                time.sleep(MINUTE)

            logging.warning(msg="resuming requests")
            continue

        elif json_data == 500:
            logging.warning(msg="500 code received at: {country} at position {position}".format(
                country=country,
                position=i
            ))
            continue

        else:
            logging.debug(msg="skipped for reason: {reason}".format(reason=json_data))
            i -= 1
            continue

