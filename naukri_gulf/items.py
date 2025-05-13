import scrapy

class Product(scrapy.Item):
    search_term = scrapy.Field()
    passport_no = scrapy.Field()
    data = scrapy.Field()
    input_data = scrapy.Field()
    scrape_date = scrapy.Field()
