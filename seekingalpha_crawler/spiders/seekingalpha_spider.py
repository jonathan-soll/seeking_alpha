import scrapy

class SeekingAlpha_Spider(scrapy.Spider):
    name = "seekingalpha"
    start_urls = [
            'https://seekingalpha.com/market-news/all',
    ]
    def parse(self, response):
        '''
        Handles the response downloaded for each of the requests made.

        parameters:
        response - instance of TextResponse that holds the page content and has
                    further helpful methods to handle it.
        '''

        for news in response.xpath('//li[@class="mc"]'):
            yield {
                'symbol': news.xpath('div[@class="media-left"]').css('a::text').extract_first(),
                'title': news.xpath('div[@class="media-body"]').xpath('div[@class="title"]').css('a::text').extract_first()
            }
