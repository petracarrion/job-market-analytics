import scrapy

settings = {
}


class StepStoneSpider(scrapy.Spider):
    name = "stepstone"
    max_pages = 5

    def get_urls(self):
        job_title = 'Python'

        url = f'https://www.stepstone.de/jobs/{job_title}'

        return [url]

    def start_requests(self):
        urls = self.get_urls()
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        self.log('Getting item from StepStone')
        self.log(response)
        self.log(response.body)
        # page = response.url.split("/")[-2]
        # filename = f'quotes-{page}.html'
        # with open(filename, 'wb') as f:
        #     f.write(response.body)
        # self.log(f'Saved file {filename}')
