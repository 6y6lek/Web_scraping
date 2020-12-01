import requests
from bs4 import BeautifulSoup
import sqlite3 as sq


URL = 'https://auto.ria.com/uk/newauto/marka-jaguar/'
HEADERS = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                         'Chrome/87.0.4280.66 Safari/537.36',
           'accept': '*/*'}
HOST = 'https://auto.ria.com'


def get_html(url, params=None):
    r = requests.get(url, headers=HEADERS, params=params)
    return r

def get_pages_count(html):
    soup = BeautifulSoup(html, 'html.parser')
    pagination = soup.find_all('span', class_='mhide')
    if pagination:
        return int(pagination[-1].get_text())
    else:
        return 1


def get_content(html):
    soup = BeautifulSoup(html, 'html.parser')
    items = soup.find_all('div', class_='proposition')

    cars = []
    for item in items:
        uah_price = item.find('span', class_='grey size13')
        if uah_price:
            uah_price = uah_price.get_text().replace(' • ', '')
        else:
            uah_price = 'none'
        cars.append({
            'title': item.find('h3', class_='proposition_name').get_text(strip=True),
            'link': HOST + item.find('a').get('href'),
            'usd_price': item.find('span', class_='green').get_text(strip=True),
            'uah_price': uah_price,
            'city': item.find('div', class_='proposition_region size13').find_next('strong').get_text(strip=True),
            'engine_type': item.find('span', class_='i-block').get_text(strip=True),
            'volume': item.find('span', class_='i-block').find_next().get_text(),
            'transmission': item.find('span', class_='size13').find_next().find_next().find_next().find_next().get_text(strip=True),
        })
    return cars


def parse():
    html = get_html(URL)
    if html.status_code == 200:
        cars = []
        pages_count = get_pages_count(html.text)
        for page in range(1, pages_count + 1):
            print(f'Parsing page {page} from {pages_count}...')
            html = get_html(URL, params={'page': page})
            cars.extend(get_content(html.text))
        return cars
    else:
        print('Error')

def Db(items):
    connection = sq.connect("cars.db")
    cursor = connection.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS cars_tb1(
    id INTEGER,
    title TEXT,
    link TEXT,
    usd_price INTEGER,
    uah_price INTEGER
    )""")
    cursor.execute("""CREATE TABLE IF NOT EXISTS cars_tb2(
    id INTEGER,
    cars1_id INTEGER,
    city TEXT,
    engine_type TEXT,
    volume TEXT,
    transmission TEXT
    )""")
    id = 1
    for item in items:
        cursor.execute("INSERT INTO cars_tb1 VALUES (?,?,?,?,?)", [id, item['title'], item['link'],
                                                                   int(item['usd_price'].replace(' ', '').replace('$', '')),
                                                                   int(item['uah_price'].replace(' ', '').replace('грн', ''))])
        cursor.execute("INSERT INTO cars_tb2 VALUES (?,?,?,?,?,?)", [id, id, item['city'], item['engine_type'],
                                                                     item['volume'], item['transmission']])
        id = id+1

    connection.commit()

    cursor.close()
    connection.close()

def Queries():
    connection = sq.connect("cars.db")
    cursor = connection.cursor()
    cursor.execute(
        """select title, link, usd_price, uah_price, city, engine_type, volume, transmission from cars_tb1
           INNER JOIN cars_tb2 on cars_tb2.cars1_id = cars_tb1.id
            """)
    cursor.execute(
        """select title, link, usd_price, uah_price, city, engine_type, volume, transmission from cars_tb1
                    INNER JOIN cars_tb2 on cars_tb2.cars1_id = cars_tb1.id
                    where usd_price > 50000 and city ="Київ"
        """)
    # cursor.execute(
    #     """select city, max(usd_price) as max_usd_price from cars_tb1
    #             INNER JOIN cars_tb2 on cars_tb2.cars1_id = cars_tb1.id
    #             GROUP by city
    #             HAVING max_usd_price < 100000
    #             ORDER by max_usd_price
    #     """)
    rows = cursor.fetchall()

    for row in rows:
        print(row)
    cursor.close()
    connection.close()


cars = []

#cars = parse()
#print(cars)
#Db(cars)

Queries()

