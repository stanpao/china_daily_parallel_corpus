# Author: Bao Junxian
# Date: 1/21/2021
import asyncio, aiohttp, logging, sys, re, time
from bs4 import BeautifulSoup
from functools import wraps
from asyncio.proactor_events import _ProactorBasePipeTransport

def silence_event_loop_closed(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except RuntimeError as e:
            if str(e) != 'Event loop is closed':
                raise
    return wrapper
_ProactorBasePipeTransport.__del__ = silence_event_loop_closed(_ProactorBasePipeTransport.__del__)

log = logging.getLogger()
log.addHandler(logging.StreamHandler(stream=sys.stdout))
log.setLevel(logging.INFO)

def parse_parallel_p(sentences):
    result = list()
    sentences = [i for i in sentences if not re.match('^.$', i)]
    start = 0
    while True:
        try:
            en, zh = sentences[start:start+2]
            if re.search('^[^\u4e00-\u9fa5]*$', en) and re.search('[\u4e00-\u9fa5]*', zh):
                result.append((en.strip(), zh.strip()))
                start += 2
            else:
                start += 1
                continue
        except ValueError:
            break

    return result

async def obtain_content(session, url):
    resp = await session.get(url)
    content = BeautifulSoup(await resp.text(encoding='utf8'), features='lxml')
    sentences = [i.text for i in content.select('.image~ p')]
    log.info(f'{time.asctime()} Parsing {url} is done.')
    return parse_parallel_p(sentences)

async def main(file):
    num = 0
    with open(file, 'w', encoding='utf8') as f:
        while True:
            async with aiohttp.client.ClientSession(raise_for_status=True) as session:
                num += 1
                url = f'https://language.chinadaily.com.cn/news_bilingual/page_{num}.html'
                resp = await session.get(url)
                item_urls = BeautifulSoup(await resp.text(encoding='utf8'), features='lxml')
                item_urls = [item['href'] for item in item_urls.select('.gy_box_txt2 a')]
                if not item_urls:
                    break
                data = await asyncio.gather(*[obtain_content(session, 'https:'+uri) for uri in item_urls])
                try:
                    for result in data:
                        for en, zh in result:
                            f.write(f'{en}\t{zh}\n')
                except ValueError:
                    pass


if __name__ == '__main__':
    asyncio.run(main('china_daily_bilingual.tsv'))

    # Below is for test
    # async def test():
    #     async with aiohttp.client.ClientSession(raise_for_status=True) as session:
    #         log.info(await obtain_content(session,
    #         'https://language.chinadaily.com.cn/a/202011/18/WS5fb4a679a31024ad0ba94eb2.html'))
    #
    # asyncio.run(test())