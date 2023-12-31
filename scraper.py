import scrapy 
from scrapy.spiders import CrawlSpider
from scrapy.crawler import CrawlerProcess 
from scrapy import Request 
from itemloaders.processors import TakeFirst
from scrapy.loader import ItemLoader
from re import findall,sub
from math import ceil 
from scrapy.shell import inspect_response
from urllib.parse import quote 
from scrapy.utils.response import open_in_browser
from scrapy.http.response.html import HtmlResponse
import json 
from math import ceil 

# part related to executable :
def warn_on_generator_with_return_value_stub(spider, callable):
    pass

scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
#---------------------------------------------------------------------------------------------------#


class DetailsItem(scrapy.Item):
    pass


class InfosSpider(scrapy.Spider):
    name = 'extractor'  

    def __init__(self,office_url:str):
        self.office_url = office_url 

    def start_requests(self):
        yield Request(
            self.office_url,
            callback=self.parse_total_pages
        )

    def parse_total_pages(self,response):
        total_pages = self.get_total_pages(response)
        for page in range(1,total_pages+1) :
            yield Request(
                response.url + f'&PageSize=20&Page={page}',
                dont_filter=True,
                callback= self.parse_listing 
            )


    def parse_listing(self,response):
        agents_urls =  [response.urljoin(url) for url in response.xpath('//a[@class="titleUnderlined"]/@href').getall()]
        for url in agents_urls :
            yield Request(
                url,
                callback=self.parse_agent
            )


    def parse_agent(self,response):
        agent_item = {}
        agent_item['agent url'] = response.url
        agent_item['agent_id'] = findall('\d+',response.url)[0]
        agent_item['agent_fullname'] = response.xpath('//h1/text()').get().strip()
        agent_item['agent_firstname'] = agent_item['agent_fullname'].split()[0] 
        agent_item['agent_lastname'] = agent_item['agent_fullname'].split(' ',1)[1] 
        agent_item['agent_phone'] = response.xpath(
            'string(//a[@class="agent-phone"]|'
            '//h1/following-sibling::div//a[contains(@href,"tel")])'
            ).get()
        agent_item['agent_street'] = self.get_street(response)
        agent_item['agent_city'] = self.get_city(response)
        agent_item['agent_state'] = self.get_state(response)
        agent_item['agent_zip'] = self.get_zip(response)
        agent_item['agent_website'] = response.xpath('//a[contains(text(),"View My Website")]/@href').get()
        agent_item['office_name'] = response.xpath(
            'string(//a[contains(text(),"Download My App")]/ancestor::div[1]'
            '/following-sibling::div[1]//strong[a])'
        ).get()
        agent_item['agent_title'] = self.get_title(response)
        agent_item['agent_email'] = ''
        if not agent_item['agent_website'] :
            yield agent_item 
        else :
            yield Request(
                agent_item['agent_website'],
                callback=self.parse_email,
                meta={
                    'agent_item':agent_item
                }
            )


    def parse_email(self,response):
        # if 'Email Me' in response.text :
        #     breakpoint()
        agent_item = response.meta['agent_item']
        agent_item['agent_email'] = response.xpath('//a[contains(@href,"mailto")]/@href').get().replace('mailto:','') \
            if  response.xpath('//a[contains(@href,"mailto")]/@href') else ''
        yield agent_item


    def get_total_pages(self,response:HtmlResponse) -> int :
        return int(response.xpath('string(//ul[contains(@class,"pagination")]/li[last()-1])').get().strip())


    def get_address(self,response:HtmlResponse) -> str :
        addrs_parts = response.xpath(
            '//a[contains(text(),"Download My App")]/ancestor::div[1]'
            '/following-sibling::div[1]//strong[a]//following-sibling::text()'
        ).getall()
        return ' '.join(
            [
                part.strip() for part in addrs_parts if part.strip()
            ]
        ).replace('Office:','')
    

    def get_street(self,response:HtmlResponse) -> str :
        addrs_parts = [ 
            part.strip() 
            for part in response.xpath(
                '//a[contains(text(),"Download My App")]/ancestor::div[1]'
                '/following-sibling::div[1]//strong[a]//following-sibling::text()'
            ).getall()
        ]
        for part in addrs_parts : 
            if findall('^\d+',part) :
                return part 


    def get_city(self,response:HtmlResponse) -> str: 
        return response.xpath(
            '//a[contains(text(),"Download My App")]/ancestor::div[1]'
            '/following-sibling::div[1]//strong[a]//following-sibling::text()'
            ).re_first('([\s\S]+),').strip()


    def get_state(self,response:HtmlResponse) -> str :
        return findall('[A-Z][A-Z]',self.get_address(response))


    def get_zip(self,response:HtmlResponse) -> str: 
        return findall('\d{5}',self.get_address(response))


    def get_title(self,response:HtmlResponse) -> str :
        return sub('\s{2,}','\n',response.xpath('string(//h1/span)').get().strip())


if __name__ == '__main__':
    office_url = input('enter the office url : ')
    process = CrawlerProcess(
        {
            #'LOG_LEVEL':'ERROR',
            'CONCURENT_REQUESTS':4,
            'DOWNLOAD_DELAY':0.5,
            #'HTTPCACHE_ENABLED' : True,
            'FEED_URI':'output.csv',
            'FEED_FORMAT':'csv',
        }
    )



    process.crawl(InfosSpider,office_url)
    process.start()

