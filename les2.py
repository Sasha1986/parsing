import requests
from pathlib import Path
import urljoin
import bs4
import pymongo


MONTHS = {
    "янв": 1,
    "фев": 2,
    "мар": 3,
    "апр": 4,
    "май": 5,
    "июн": 6,
    "июл": 7,
    "авг": 8,
    "сен": 9,
    "окт": 10,
    "ноя": 11,
    "дек": 12,
}

class ParseMagnit:

    def __init__(self, start_url, db_client):
        self.start_url = start_url
        self.db = db_client["gb_data_miner_16_02_2021"]

    def _get_response(self, url):
        try:
            response = requests.get(url)
            return response
        except AttributeError:
            pass

    def _get_soup(self,url):
        try:
            response = self._get_response(url)
            soup = bs4.BeautifulSoup(response.text, "lxml")
            return soup
        except AttributeError:
            pass

    def _template(self):
        return {
            "product_name": lambda a: a.find("div", attrs={'class': "card-sale__title"}).text,
            "url": lambda a: urljoin(self.start_url, a.attrs.get('href', "")),
            "promo_name": lambda a: a.find("div", attrs={'class': "card-sale__name"}).text,
            "old_price": lambda  a: float(
                ".".join(
                    itm for itm in a.find("div", attrs={'class': "label__price_old"}).text.split()
                )
            ),
            "new_price": lambda a: float(
                ".".join(
                    itm for itm in a.find("div", attrs={'class': "label__price_new"}).text.split()
                )
            ),
            "image_url": lambda a: urljoin(self.start_url, a.find("img").attrs.get("data-scr")),
            "date_from": lambda a: self.__get_date(
                a.find("div", attrs={"class": "card-sale__date"}).text
            )[0],
            "date_to": lambda a: self.__get_date(
                a.find("div", attrs={"class": "card-sale__date"}).text
            )[1],
        }
    def __get_date(self, date_string) -> list:
        date_list = date_string.replace("с ", "", 1).replace("\n", "").split("до")
        result = []
        for date in date_list:
            temp_date = date.split()
            result.append(
                temp_date.datetime(
                    year = temp_date.datetime.now().year,
                    day=int(temp_date[0]),
                    month=MONTHS[temp_date[1][:3]]
                )
            )
        return result

    def run(self):
        soup = self._get_soup(self.start_url)
        catalog = soup.find("div", attrs={"class": "catalogue__main"})
        for product_a in catalog.find_all("a", recursive = False):
            product_data = self._parse(product_a)
            self.save(product_data)

    def _parse(self, product_a:bs4.Tag) -> dict:
        product_data = {}
        for key, funk in self._template().items():
            try:
                product_data[key] = funk(product_a)
            except AttributeError:
                pass

        return product_data

    def save(self, data:dict):
        collection = self.db["magnit"]
        collection.insert_one(data)

if __name__ == '__main__':
    url = "https://magnit.ru/promo/"
    db_client = pymongo.MongoClient("mongodb://localhost:27017")
    parser = ParseMagnit(url, db_client)
    parser.run()