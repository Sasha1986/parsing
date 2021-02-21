from pathlib import Path
import requests
import time
import json

class Parse5ka:
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0"
    }

    def __init__(self, start_url: str, products_path: Path):
        self.start_url = start_url
        self.products_path = products_path

    def _get_response(self, url):
        while True:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                return response
            time.sleep(0.5)

    def run(self):
        for product in self._parse(self.start_url):
            product_path = self.products_path.joinpath(f"{product['id']}.json")
            self._save(product, product_path)

    def _parse(self, url):
        while url:
            response = self._get_response(url)
            data = response.json()
            url = data['next']
            for product in data["results"]:
                yield product

    @staticmethod
    def _save(data: dict, file_path):
        jdata = json.dumps(data, ensure_ascii=False)
        file_path.write_text(jdata, encoding="UTF-8")

    def get_dir_path(dirname: str) -> Path:
        pass

if __name__ == '__main__':
    url = "https://5ka.ru/api/v2/special_offers/"
    save_path = Path(__file__).parent.joinpath("products")
    if not save_path.exists():
        save_path.mkdir()

    parser = Parse5ka(url, save_path)
    parser.run()