import scrapy
from naukri_gulf.items import Product
from lxml import html
import os
import pandas as pd
import requests
from datetime import date

class Naukri_gulfSpider(scrapy.Spider):
    name = "naukri_gulf"
    start_urls = ["https://example.com"]

    def parse(self, response):
        script_directory = os.path.dirname(os.path.abspath(__file__))

        # Find the CSV file that ends with 'ENBD-TRACING_1.csv'
        csv_files = [f for f in os.listdir(script_directory) if f.endswith('ENBD-TRACING_3.csv')]
        
        if not csv_files:
            print("No matching CSV file found.")
            return

        # Use the first matching file
        csv_path = os.path.join(script_directory, csv_files[0])

        # Read it using pandas
        df = pd.read_csv(csv_path, low_memory=False).to_dict('records')
        
        cookies = {
            'aka_location': 'Country=IN',
            'aka_location': 'Country=IN',
            '_t_ds': '45b16afe1746248928-6045b16afe-045b16afe',
            'isJsLoggedIn': 'false',
            'profileCom': 'n',
            'chatbotonorganicresman': 'n',
            'chatbotonmarketingresman': 'y',
            'chatbotonmarketingresmanAr': 'y',
            'chatbotonorganicresmanAr': 'n',
            'NGSessionId': '03cc3dbac939',
            '__zlcmid': '1RTnaMtH7zb86r3',
            'PHPSESSID': 'bd58d22f4493bcbeb9a31e10202c7dab',
            'ng_ecom': 'a',
            'countryc': 'IN',
            'countryn': 'India',
            'city': 'Kochi',
            'state': 'Kerala',
            'aka_location': 'Country=IN',
            'NGStaticTpl': '1',
            '_ngenv1[lang]': 'en',
            'puppeteer': 'FALSE',
            'resmanexp': '',
            'mboost': 'false',
            'mboost50': 'false',
            'mboost50Mob': 'false',
            'expreco': 'C',
            'topempexp': 'A',
            'newParser': 'y',
            'nimnr_new': 'false',
            'LOCA': 'ae09d9c71b234e159e52b676c809280201fcb58c9509187d',
            'NIMNR_INDUS_ID': '0d30acfdaf918f51a17b2c9a709e12976f37353c3155a934',
            'NIMNR_PRODTYPE': '6e5f4dd7b4604e52b8b7a6e0c72d13251b1f8731bebcc731',
            'NIMNR_LTS': '6162f3f210593a910d26aecd9b95c62626932d9bac02934e85091bc7442fa0da5fbfee560b95752f3dac5c76f8eb421c',
            'rm_new': 'true',
            'NIMNR_COMPANY': '76fcef01357d7e9f677463f83be3c5547536016f85658201c7f65a9a7a3c16f5',
            'NIMNR_CMID': '5c6ddcfe91d318d3c3e674c2e386e61176e29829c0ac9cca',
            'show_mjr': 'true',
            'NIMNR_INDUS': '137ab16308803e888d1e5ef1db3bbe7cc6cce18380e2c01f8e636764ae59351a6d68c9ebc0967ce9',
            'NIMNR_TLBR': 'expded',
            'NIMNR_TLBRNOTIFY': 'show',
            'ng_jp': 'new',
            'NIMNR_USER': 'd31c2189e6876cd82ebe5bf1f140d5008a01118378c46e853657ca3e5d40f8bbb6d4e194d7b92f61',
            'NIMNR_UID': 'bc4ba18231743c527bf1a3f4881893fd3baeae8f7f9a4779',
            'NIMNR_WELCOME_BACK': '93b9f7a5982b2fcef4bff82d9bbf3d5bcb23d41b36e8cdb28941a1a542044b67d02bab66c1994f755fa60fb2b5e1934b2df775486112ebefa98256e771fada608182f824a0a8639f9d357d85dd015b918a01118378c46e853657ca3e5d40f8bb66692bb83437616878226d7e3251f162365866cf3406b8f37b8db348d2238637b99283984522337f269f4a39258e00a399db61772231301435781a6a740f0619e9a73af578c03b399dd098b4ca056d094c951e802d91e1d6c722db2c28a64ebb1f7874e62e3b4c770d9aea1d47ea87e626aac733e5a1a9b9ff7f74a39082797e929fb66d990414f4504d57ac0ed885c83bdbd13a278f29e012cf0e7a98effb27671354223547458b7ec8ea0ee82fe2a05c2389943631af7efb448188b8e66c8ee567452c4cbc4953aedd51533ce294b6bb3ea6f6b5f7841e6cfcb1de3eee7b6111a93aff4227355bd2bfb900ee692e5dcb0f2583b5a31435e810f7b43d070d1a2b3733940793004931f10b5d9ac13402eafa7747c1b31b2b3640126b30b1d2bb7a3e36c178b15dc963d2bede13eeb3c306082909e27fc29c95b02431abe9b7de252fb72e5755afb7c11e834dacf88b5c9a3f53b7f9116ca3cbb729c0869e1810e8d22b6b2b6397a280e37d95d6411e162d4ff1722ae60318d52f9592e474be1c0b0020cd0bfb0b854e11472be8a614a9',
            'vt': '275bad0f39722768a9d2f7ef2e7777abd422412b828b33bfdc6322a46fda9e3c3cc5cff75edfb1cc6997add858748e25a88b06342c96a2ec0ae6ca520b9e2ed89bd14949bb2aab8f7c71b3a5ba1d386bd41300b76680ad83c12dac2e7263733b95d54c1716bcb0416fcbc9491d931deea791482d1b541319402ea7ca988338c52d64036302545c7fd66a7d49f080af9f307031d360a40216b72b2b636aadde8a895ec03da7513c2c0c02acde45ea0f6926063627e9fe257fb970e3b524a1ce98',
            'ak_bmsc': 'A767302D79AC0F913896049A9CD327E1~000000000000000000000000000000~YAAQyEvSF6Jb+4iWAQAAmzHwzBsUlfGF7fgR6CZWizaa+rUMxwQlWBtHM104GQkeQs15xBWEAH04jLaSUrlvS/B23HRIJi5rKN1BJO6WYOXa29RY01tCtK6j77681FozV2t6fxDxNNEXKdLy7MxD+S7gbM9OyQoAAVdmIR0/evFNONH7+0AesNTUDKPGUoE3SXOwnK6Q+4MeTPt4UlIQ/cbV3xqPGmmMZ7HF4U8ceBbZXQ/vewAZ63056ojnbnCaI+6vDGr2PkkdrC180tJYi4wGIWFQ+A5w9vFdS9NMJMheTnvS/obXJdtME4ofiYc9k9kaBpFddXOLvX+M4mO5v8lHeasr8Gc3NjeDCNzeaSVqchS2b+wKjbN0uMpEO4yZCsF0iQrz6znnbryqyA/P',
            'MNR_AT': 'JjRALF1HfisSrPSZVEnXtA86fUhVT67QVEdgpyyG',
            'MNR_RT': 'P3wYiRnF6cZdWQghqG0dWJJsGQQTvwsKmo5tOBcP',
            'IsLoggedIn': '1',
            'NIMNR_CON': 'JjRALF1HfisSrPSZVEnXtA86fUhVT67QVEdgpyyG',
            'NIMNR_TM': '1747195019502',
            'bm_mi': '9050F8777408B63A8F9E210C52A43182~YAAQyEvSF0lc+4iWAQAAjEHwzBvw1e16HvSGVapadXE6pkr3mBvJZpyyT6jAXY+xrapc6cEU5Vc5w6H3yc7YpLBbdeQsMFlax2oZ53o4rLRJGuFDE2u10t8cO48mN2SFtBYzno0Iu/a5+aXCgNCsYFXLyFB91zrqK89YMFoVef1hda3EALLU9EeT1if4bU5LA41nixv6LG9Tn5gQ3G1C23zjCj5iyhM0iYNwWv4denIuupGbBQguUVJQeL1WX+pBx10LfpIu97H+QWbmkEPEToXH7m402X1q3KbhZ3eCnQAZPhQks4aW+g1sGw/fvPHgzkBhgDWwCckr/TTWGuHWhw==~1',
            'JSESSIONID': '802113C65BA248427AD6F31A4EF079FD',
            'bm_sv': '01C9019DA0CD5B650768D5F1A4790F1B~YAAQyEvSF/Vo+4iWAQAA0VjxzBvlcqEPFFF9NDBLAxAUDeTchZVJpLNliPKFCnx1UUDfVp3rGA6uS7GKBtxhJQruhS2c28qmQTe9YOk6Yva85JKEqBefJB8i4URuIfEYFU8uaHUpdWJsd8TbLLlbnn0j5NcgPZnFn8JM42tHCRx7RYU9m2WqPtzONhi5WwQfohWAYDfGdDkCWgBANH+156LlPY+kzekTNSHWbc5wIc4zIdeQ7wCCX7Evsf6Dgy/yVPflfFQ=~1',
        }

        headers = {
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9',
            'appid': '111',
            'companyid': '208393',
            'content-type': 'application/json;charset=UTF-8',
            'origin': 'https://www.naukrigulf.com',
            'priority': 'u=1, i',
            'recruiterid': '247489',
            'referer': 'https://www.naukrigulf.com/recruiter/resdex/searchform',
            'sec-ch-ua': '"Not.A/Brand";v="99", "Chromium";v="136"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'systemid': '1111',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
            # 'cookie': 'aka_location=Country=IN; pwa_lang=en; countryc=IN; countryn=India; city=Kochi; state=Kerala; _t_ds=23f212f91747135278-2023f212f9-023f212f9; aka_location=Country=IN; isJsLoggedIn=false; _ngenv1[lang]=en; puppeteer=FALSE; resmanexp=; mboost=false; mboost50=false; mboost50Mob=false; expreco=C; topempexp=A; newParser=y; profileCom=n; chatbotonorganicresman=n; chatbotonmarketingresman=y; chatbotonmarketingresmanAr=y; chatbotonorganicresmanAr=n; _ga=GA1.1.1572877766.1747135279; _fbp=fb.1.1747135279152.20246519576923330; ak_bmsc=0D85056F79509AA4F90BBDE9B881940F~000000000000000000000000000000~YAAQyEvSF4no7oiWAQAASrFgyRuzeBnyKtz8JX3dlzXuts44v1ab8l1MU7aJIz6iyy7A4rOGwsQm1YaK0iGxFjKYzpLlAzYUToEjFwp403N+016AFdPqZcgSs0ac1pjddY/QGEHdbJlB8jcmSvfev97EfWMcKRMr7+savQihkUs8/kq9dKVaAUYdONEBjXPBMmiUNZgLJxFyvL3RFc6D+nWJNFIEyTdaqwn4yGevw0A2R28V7aoHpdvQ+l8QGIuoKNWnxI6Tij9UQDGYe9dRlGYOwYxFn2n1f4zg0W2XRoBtcZEi6v8ierVGq7hd0KqX6/97SINmyg/3JzhQr3FOxNKLMXAxrGBOncKI6MIJNS/udf1kr5uV3U3HFYRXrykbV1FjLn5TiWzKiwUTZE9i4ICz7Sxn8ZWh/04+cuvQeuRfiAETvND/pdVR8ILA8FOUc268m22j8Fpunx4QaafpJWNJ; _ga_Q866Y8JT5J=GS2.1.s1747135278$o1$g1$t1747135289$j0$l0$h0; PHPSESSID=b1bf05f6724d7c6a7cde430f129b4b26; NGSessionId=67d357ef4f22; ng_ecom=b; NGStaticTpl=1; __utmt=1; __insp_wid=1563999834; __insp_slim=1747135291344; __insp_nv=true; __insp_targlpu=aHR0cHM6Ly93d3cubmF1a3JpZ3VsZi5jb20vZW1wbG95ZXItcmVjcnVpdG1lbnQtc29sdXRpb25z; __insp_targlpt=Sm9iIFBvc3RpbmcsIENWIFNlYXJjaCAtIE9ubGluZSByZWNydWl0bWVudCBhdCBOYXVrcmlndWxm; __insp_norec_sess=true; __zlcmid=1RdnbvdbR001mg5; NIMNR_PRODTYPE=6e5f4dd7b4604e52b8b7a6e0c72d13251b1f8731bebcc731; NIMNR_LTS=6162f3f210593a910d26aecd9b95c62626932d9bac02934e85091bc7442fa0da5fbfee560b95752f3dac5c76f8eb421c; nimnr_new=false; rm_new=true; MNR_AT=2EaI5Yh5fOw53JTcI5dLXVPFZ8Rta0qoQmBBIob3; MNR_RT=ihKTiUjZoe7ULazjjCKUvhyD7bdPAJbCRh2R07K0; IsLoggedIn=1; NIMNR_CON=2EaI5Yh5fOw53JTcI5dLXVPFZ8Rta0qoQmBBIob3; NIMNR_TM=1747135305844; NIMNR_COMPANY=76fcef01357d7e9f677463f83be3c5547536016f85658201c7f65a9a7a3c16f5; NIMNR_USER=d31c2189e6876cd82ebe5bf1f140d5008a01118378c46e853657ca3e5d40f8bbb6d4e194d7b92f61; LOCA=ae09d9c71b234e159e52b676c809280201fcb58c9509187d; NIMNR_UID=bc4ba18231743c527bf1a3f4881893fd3baeae8f7f9a4779; NIMNR_CMID=5c6ddcfe91d318d3c3e674c2e386e61176e29829c0ac9cca; show_mjr=true; NIMNR_INDUS=137ab16308803e888d1e5ef1db3bbe7cc6cce18380e2c01f8e636764ae59351a6d68c9ebc0967ce9; NIMNR_INDUS_ID=0d30acfdaf918f51a17b2c9a709e12976f37353c3155a934; NIMNR_TLBR=expded; NIMNR_TLBRNOTIFY=show; ng_jp=new; NIMNR_WELCOME_BACK=93b9f7a5982b2fcef4bff82d9bbf3d5bcb23d41b36e8cdb28941a1a542044b67d02bab66c1994f755fa60fb2b5e1934b2df775486112ebefa98256e771fada608182f824a0a8639f9d357d85dd015b918a01118378c46e853657ca3e5d40f8bb66692bb83437616878226d7e3251f162365866cf3406b8f37b8db348d2238637b99283984522337f269f4a39258e00a399db61772231301435781a6a740f0619e9a73af578c03b399dd098b4ca056d094c951e802d91e1d6c722db2c28a64ebb1f7874e62e3b4c770d9aea1d47ea87e626aac733e5a1a9b9ff7f74a39082797e929fb66d990414f4504d57ac0ed885c83bdbd13a278f29e012cf0e7a98effb27671354223547458b7ec8ea0ee82fe2a05c2389943631af7efb448188b8e66c8ee567452c4cbc4953aedd51533ce294b6bb3ea6f6b5f7841e6cfcb1de3eee7b6111a93aff4227355bd2bfb900ee692e5dcb0f2583b5a31435e810f7b43d070d1a2b3733940793004931f10b5d9ac13402eafa7747c1b31b2b3640126b30b1d2bb7a3e36c178b15dc963d2bede13eeb3c306082909e27fc29c95b02431abe9b7de252fb72e5755afb7c11e834dacf88b5c9a3f53b7f9116ca3cbb729c0869e1810e8d22b6b2b6397a280e37d95d6411e162d4ff1722ae60318d52f9592e474be1c0b0020cd0bfb0b854e11472be8a614a9; HOWTORT=cl=1747135293949&r=https%3A%2F%2Fwww.naukrigulf.com%2Femployer-login%3Fauth%3DactiveSession%26actionSrc%3Dmodal%26username%3Dbyju.krishna%40fynkom.com&nu=javascript%3Avoid(0)%3B&ul=1747135305875&hd=1747135306005; __utma=127812882.1572877766.1747135279.1747135306.1747135306.1; __utmc=127812882; __utmz=127812882.1747135306.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); vt=c6eaa43ce6e092993bd3afc30c11e251d422412b828b33bfdc6322a46fda9e3c3cc5cff75edfb1cc6997add858748e25a88b06342c96a2ec0ae6ca520b9e2ed89bd14949bb2aab8f7c71b3a5ba1d386bd41300b76680ad83c12dac2e7263733b95d54c1716bcb0416fcbc9491d931deea791482d1b541319402ea7ca988338c52d64036302545c7f13762fb09b8902d2307031d360a4021606616ee45a267130488c1aa23e044881cffe1853c3b5c3bcc3b2a706df5c6f47b970e3b524a1ce98; JSESSIONID=35DA1651885E6EBAA20108F4B3A2081A; __utmb=127812882.14.0.1747135308581; _ga_2G8SVNJJLW=GS2.1.s1747135279$o1$g1$t1747135309$j30$l0$h0; bm_sv=3A693F352205839F1F002D61E0087743~YAAQyEvSF5ft7oiWAQAAcjFhyRttY+NYsGtK4LafrX4ioNoP5yWTMpBOxMsJJdMDx+a8JfsELYNJtM/7pMT1T5KISo6pYLRht9ckMZJdQsWFCvTFj+8qb6fNqzdMg4Jn94XHnpn7LcW9JtsaY3o0Rw9OdXm5tKRksziEjYfhMXsZwNDIM4RwlGeKLPtK6ZjHGIeOPUoQK7Dc9D9MZMbtb2gj962yEW3gxppvDuxN1ATi/YLdux1iMOHSlwFGl0hw9RJMZb8=~1; _ga_R0274FSNNG=GS2.1.s1747135290$o1$g1$t1747135324$j0$l0$h0; _gcl_au=1.1.956207579.1747135279.360919679.1747135297.1747135324',
        }

        json_data = {
            'keyword': {
                'any': [
                    'Alshengity_a@hotmail.com',
                ],
            },
            'employmentDetails': {
                'minExp': None,
                'maxExp': None,
                'convertToCurrency': '3',
                'minCtc': None,
                'maxCtc': None,
                'prevDesig': None,
                'farea': None,
                'indType': None,
                'prevOrgn': None,
            },
            'additionalDetails': {
                'gender': None,
                'minAge': None,
                'maxAge': None,
                'drivingLic': None,
                'langsKnown': None,
                'langType': 'or',
                'updatedWithIn': '0',
                'drivingLicCountry': None,
            },
            'nmFlags': {},
            'educationalDetails': {},
            'source': 'search',
        }
        for item in df:
            search_term = ''
            pass_port_no = item.get('PASSPORTNO')
            input_data = str(item)
            customer_name = item.get('CUSTOMER NAME')
            res_email = item.get('RES_EMAIL.1', '')
            scrape_date = date.today()
            per_email = item.get('PER_EMAIL.1')
            if res_email and '@' in str(res_email):
                search_term = res_email
            elif per_email and '@' in str(per_email):
                search_term = per_email
            elif customer_name:
                search_term = customer_name
            # print(search_term)
            json_data['keyword']['any'][0] = f"{search_term}"
            # json_data['keyword']['any'][0] = "alshengity_a@hotmail.com"
            if search_term:
                res = requests.post(
                    'https://www.naukrigulf.com/rdxapi/resdex/searchform',
                    cookies=cookies,
                    headers=headers,
                    json=json_data
                    )
                try:
                    data_items = res.json()
                except Exception as error:
                    data_items = ''
                items = {
                    'data': str(data_items),
                    'passport_no': str(pass_port_no),
                    'input_data': input_data,
                    'search_term': str(search_term),
                    'scrape_date': str(scrape_date)
                }
                yield Product(**items)
                # return
