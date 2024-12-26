import json, csv, datetime, asyncio, time
from aiohttp import ClientSession
from bs4 import BeautifulSoup
with open('url.txt', 'r') as file:
    url = file.readline()
all_items = []
books_data = []
pages_tasks = []
items_tasks = []
urlses = []
cur_time = ''
count = 0
async def tasks_for_pages(url):
    headers = {
        'Accept': '*/*',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0'
    }
    async with ClientSession() as session:
        async with session.get(url=url, headers=headers) as response:
            soup = BeautifulSoup(await response.text(), 'lxml')
            pages_count = int(soup.find_all('div', class_='pagination-number')[-1].text)
            for page in range(1, pages_count + 1):
                task = asyncio.create_task(get_items(url, session, page))
                pages_tasks.append(task)
                await asyncio.gather(*pages_tasks)
async def get_items(url, session, page):
    global all_items
    headers = {
        'Accept': '*/*',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0'
    }
    url = url + f'?page={page}'
    async with session.get(url, headers=headers) as response:
        soup = BeautifulSoup(await response.text(), 'lxml')
        items = soup.find_all('div', class_='genres-carousel__item')
        for item in items:
            all_items.append(item)
async def tasks_for_items(items):
    async with asyncio.TaskGroup() as group:
        for item in items:
            group.create_task(get_data(item))
async def get_data(item, retry=5):
    global count
    headers = {
        'Accept': '*/*',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0'
    }
    url = 'https://www.labirint.ru' + item.find('a', class_='product-title-link').get('href')
    if url not in urlses:
        urlses.append(url)
    else:
        return
    try:
        async with ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                time.sleep(0.0)
                soup = BeautifulSoup(await response.text(), 'lxml')
                try:
                    availability = soup.find('div', class_='prodtitle-availibility').find('span').text
                except:
                    availability = 'no info'
                try:
                    title = item.find('span', class_='product-title').text.strip()
                except:
                    title = 'Нет названия книги'
                try:
                    book_authors = item.find('div', class_='product-author').find_all('a')
                    book_author = ', '.join([str(ba.get('title')) for ba in book_authors])
                except:
                    book_author = 'Нет автора'
                try:
                    pubhouses = item.find('div', class_='product-pubhouse').find_all('a')
                    pubhouse = ': '.join(str(ph.get('title')) for ph in pubhouses)
                except:
                    pubhouse = 'Нет издательства'
                try:
                    new_price = int(
                        item.find('div', class_='price').find('span', class_='price-val').text[:-5].strip().replace(' ',
                                                                                                                    ''))
                except:
                    new_price = 'Нет новой цены'
                try:
                    old_price = int(
                        item.find('div', class_='price').find('span', class_='price-gray').text.strip().replace(' ', ''))
                except:
                    old_price = 'Нет старой цены'
                try:
                    book_sale = round((1 - new_price / old_price) * 100)
                except:
                    book_sale = 'Нет скидки'
                books_data.append(
                    {
                        'book_title': title,
                        'book_author': book_author,
                        'publishing_house': pubhouse,
                        'new_price': new_price,
                        'old_price': old_price,
                        'book_sale': book_sale,
                        'availability': availability,
                        'url': url
                    }
                )
                with open('urls_prog.txt', 'a') as file:
                    file.write(url + '\n')
                with open(f'labirint_{cur_time}.csv', 'a', encoding='utf-8-sig', newline='') as file:
                    writer = csv.writer(file, delimiter=';')
                    writer.writerow(
                        (
                            title,
                            book_author,
                            pubhouse,
                            new_price,
                            old_price,
                            book_sale,
                            availability,
                            url
                        )
                    )
                count += 1
                print(f'[+] {count}/{len(urlses)}: {url} {soup.find('div', class_='prodtitle').find('h1').text.strip()}')
                print('-'*20)
    except Exception as ex:
        if retry == 0:
            print(f'[FAIL] url = {url}')
        if retry:
            try:
                time.sleep(30)
                print(f'[INFO] retry = {retry} => {url}')
                async with asyncio.TaskGroup() as group:
                    group.create_task(get_data(item, retry=(retry-1)))
            except:
                pass
        else:
            raise

def main():
    global cur_time
    start_time = time.time()
    cur_time = datetime.datetime.now().strftime('%d_%m_%Y_%H_%M')
    with open(f'labirint_{cur_time}.csv', 'w', encoding='utf-8-sig', newline='') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(
            (
                'Название книги',
                'Автор',
                'Издательство',
                'Цена со скидкой',
                'Цена без скидки',
                'Процент скидки',
                'Наличие',
                'Ссылка'
            )
        )
    asyncio.run(tasks_for_pages(url))
    asyncio.run(tasks_for_items(all_items))
    with open(f'labirint_{cur_time}.json', 'w', encoding='utf-8') as file:
        json.dump(books_data, file, indent=4, ensure_ascii=False)

    print(f'Sript has been running: {round(time.time() - start_time, 1)} secs.')
if __name__ == '__main__':
    main()