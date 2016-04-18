import pymongo
from wtwcom import comtrade_api
import time
import logging

logging.basicConfig(filename='com_trade.log', level=logging.DEBUG)

INTERVAL_BETWEEN_REQUESTS = 7
MINUTE = 60
MINUTES_BETWEEN_409_REQUESTS = 65

COMMODITY = "TOTAL"

client = pymongo.MongoClient()
COLLECTION = client.comtrade.total

country_dict = list(client.comtrade.country_codes.find({}, {"id": 1, "_id": 0}))
country_codes = [code["id"] for code in country_dict if "all" not in code["id"]]


def is_in_db(country_cd, com_code, year_check):
    return True if COLLECTION.find_one({"rtCode": int(country_cd), "cmdCode": com_code, "yr": year_check}) else False


for year in range(1962, 2015):
    logging.info("-*"*10)
    logging.info(msg="Starting Year: {year}".format(year=year))
    logging.info("-*"*10)
    i = len(country_codes)-1

    # i = START_POSITION
    while i >= 0:
        country = country_codes[i]
        if is_in_db(country, COMMODITY, year):
            i -= 1
            logging.debug("already in DB, country: {country}, year: {year}".format(
                country=country,
                year=year)
            )
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
        if type(json_data) is list:
            res = COLLECTION.insert_one({"rtCode": int(country), "cmdCode": COMMODITY, "yr": year, "pfCode": "S1"})
            logging.debug("inserted empty entry in MongoDB at (Object_ID): "+str(res.inserted_id))
            i -= 1
        elif json_data == 409:
            logging.warning(msg="409 code received at: {country} at position {position},sleeping for {sleep}".format(
                country=country,
                sleep=MINUTES_BETWEEN_409_REQUESTS,
                position=i
            ))
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
