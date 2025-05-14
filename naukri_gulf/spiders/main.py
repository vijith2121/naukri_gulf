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
            'ak_bmsc': 'A767302D79AC0F913896049A9CD327E1~000000000000000000000000000000~YAAQyEvSF6Jb+4iWAQAAmzHwzBsUlfGF7fgR6CZWizaa+rUMxwQlWBtHM104GQkeQs15xBWEAH04jLaSUrlvS/B23HRIJi5rKN1BJO6WYOXa29RY01tCtK6j77681FozV2t6fxDxNNEXKdLy7MxD+S7gbM9OyQoAAVdmIR0/evFNONH7+0AesNTUDKPGUoE3SXOwnK6Q+4MeTPt4UlIQ/cbV3xqPGmmMZ7HF4U8ceBbZXQ/vewAZ63056ojnbnCaI+6vDGr2PkkdrC180tJYi4wGIWFQ+A5w9vFdS9NMJMheTnvS/obXJdtME4ofiYc9k9kaBpFddXOLvX+M4mO5v8lHeasr8Gc3NjeDCNzeaSVqchS2b+wKjbN0uMpEO4yZCsF0iQrz6znnbryqyA/P',
            'NIMNR_PRODTYPE': '6e5f4dd7b4604e52b8b7a6e0c72d13251b1f8731bebcc731',
            'NIMNR_LTS': '6162f3f210593a910d26aecd9b95c62626932d9bac02934e85091bc7442fa0da5fbfee560b95752f3dac5c76f8eb421c',
            'rm_new': 'true',
            'MNR_AT': 'qF0EgYigrlzHVMLJCzsK57Cfujj7NWMpYynylj2c',
            'MNR_RT': 'rLUa6dxAnjFAXgZLiCMQeZ2JzSdqetwLHqXNlVg3',
            'IsLoggedIn': '1',
            'NIMNR_CON': 'qF0EgYigrlzHVMLJCzsK57Cfujj7NWMpYynylj2c',
            'NIMNR_TM': '1747196116313',
            'NIMNR_COMPANY': '76fcef01357d7e9f677463f83be3c5547536016f85658201c7f65a9a7a3c16f5',
            'NIMNR_USER': '0efc2ec988de0f423de9a3be4ba7fd91135800ce473613f9bb04be059ae9c8570b0020cd0bfb0b8503d854324006f94c',
            'NIMNR_UID': 'fc2b5ea98929ddcedb53577fcadd2eb4526ead5cd594b94f',
            'NIMNR_CMID': '5c6ddcfe91d318d3c3e674c2e386e61176e29829c0ac9cca',
            'show_mjr': 'true',
            'NIMNR_INDUS': '137ab16308803e888d1e5ef1db3bbe7cc6cce18380e2c01f8e636764ae59351a6d68c9ebc0967ce9',
            'NIMNR_TLBR': 'expded',
            'NIMNR_TLBRNOTIFY': 'show',
            'ng_jp': 'new',
            'NIMNR_WELCOME_BACK': '31aac91ba189560f893f27c7ca3bd0a0cb23d41b36e8cdb28941a1a542044b67d02bab66c1994f755fa60fb2b5e1934b2df775486112ebefa98256e771fada608182f824a0a8639f9d357d85dd015b91135800ce473613f9bb04be059ae9c8570b0020cd0bfb0b8530c6d43a918a544329420fe9a86d6d1d59b87f590c263b476d8fa4c068a2c9ce5c516a052716a7232146baa1f3edccd8d0c1d8e4a27e745ab5316d471f7d1adf303c7db60232bf65f67fbe7c972c3f8b65bade4ae126ca57d88c57918a0f9b91f9db0524023e14e2b6694664abb07eb08a14ef08ba17f641239635b60563c31b86e79309c49fce56782d19f0df0cb362f6173114eaf0a87c80d62903c24b95b67682c4f788c09e3618fd7293d2cffcd07b6921e8e8313e0f7806c4244cf0fd44a824e12cea06c8f0b59c2eae86a2947c54feb5d36ef363f04eecc3eecad45b7840924d9721b5481b83079e9e3811591cf810f390ac33bdc701f37798d8fe31b1e01f2528ebcc4f619c17dd5499f0cb5dee30337c470ccea5e7b6f322c271733f44b30cf2c9084682b3fbd7ca94e8fc5dabb134010d0a2d6b0f2ade8f75046bb616763ed267cc4bb57d73f59bf75edb0cac8c7a9bf2fba91950c760d447a91708ac391a6e2b5ef38dbabc2f0aefd659520dd6e0847db85a90977f4a468379d0078a94db72858a335308fb946b33439fac',
            'bm_mi': '9050F8777408B63A8F9E210C52A43182~YAAQyEvSF2gf/IiWAQAA7v0AzRuLqOpCYZk0ECOgDjoirwqfGnjtm4fcQNpEFOFeEDp2lYAx9sn3lzT8hBvCuFzBuId/TEIpRf1B1PPXjW3YiA3JEZ8Iyf/l5w8NORpF8AH4UX19Dwqzcqc4yxQJvi1u6+9NdBH4LQ2DoMAZjH4++PACbJSFsUG6juVqRGj8V8ZXYG3B8C15WRGATqTRAkQXe1/Q6Am3jP/m9B09jwCnSgU4SM8W8+h1ZlObSILQubvOWCJLAJTxdhIfi9qVgkvZfGrJrShOGUHvdEmIe2Mws8x0+7GRCpDmR3JmQca51Qppv2mxntUCX5xdxsojyA==~1',
            'vt': '713200093be1d1c356524833d0189bfbd422412b828b33bf9f8da26a790ca059dc538cdab905f7ea6997add858748e25a88b06342c96a2ec0ae6ca520b9e2ed89bd14949bb2aab8f7c71b3a5ba1d386bd41300b76680ad83c12dac2e7263733b95d54c1716bcb0416fcbc9491d931deea791482d1b541319546f1e395d4b32092d64036302545c7f136d6015753eee15307031d360a402169fce3c5e1ff9e8120b8429b6bd17b68aa260cc9dcdce03c5e88cfbe4057faba0b970e3b524a1ce98',
            'JSESSIONID': 'EB95747E84B5E6E821D9A886F209870E',
            'bm_sv': '01C9019DA0CD5B650768D5F1A4790F1B~YAAQyEvSF/sj/IiWAQAA0GMBzRvtf5k4EX9xdAVGXO5vH8Ugy9fN4VWr7ycT+jYprXUoI2sBpMqIC3G/Mw85fmLkAgN5X3JhMzjHgJiypdpzKReqRKo+X4TAua9aFAxRL00ACqFGvMmj0GmYspDW5J1CQ+twp+9kgcteEWB0GwiajXnXaCQB6PHM1ONl5IdXsne1BR41gzw75NzYT4b2P5WzK8JG5Vo0KOCJStz8AsqTOSe6NWQDUn9P56IE3BQME4jiw1w=~1',
        }

        headers = {
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9',
            'appid': '111',
            'companyid': '208393',
            'content-type': 'application/json;charset=UTF-8',
            'origin': 'https://www.naukrigulf.com',
            'priority': 'u=1, i',
            'recruiterid': '222163',
            'referer': 'https://www.naukrigulf.com/recruiter/resdex/searchform',
            'sec-ch-ua': '"Not.A/Brand";v="99", "Chromium";v="136"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'systemid': '1111',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
            # 'cookie': 'aka_location=Country=IN; aka_location=Country=IN; _t_ds=45b16afe1746248928-6045b16afe-045b16afe; isJsLoggedIn=false; profileCom=n; chatbotonorganicresman=n; chatbotonmarketingresman=y; chatbotonmarketingresmanAr=y; chatbotonorganicresmanAr=n; NGSessionId=03cc3dbac939; __zlcmid=1RTnaMtH7zb86r3; PHPSESSID=bd58d22f4493bcbeb9a31e10202c7dab; ng_ecom=a; countryc=IN; countryn=India; city=Kochi; state=Kerala; aka_location=Country=IN; NGStaticTpl=1; _ngenv1[lang]=en; puppeteer=FALSE; resmanexp=; mboost=false; mboost50=false; mboost50Mob=false; expreco=C; topempexp=A; newParser=y; nimnr_new=false; LOCA=ae09d9c71b234e159e52b676c809280201fcb58c9509187d; NIMNR_INDUS_ID=0d30acfdaf918f51a17b2c9a709e12976f37353c3155a934; ak_bmsc=A767302D79AC0F913896049A9CD327E1~000000000000000000000000000000~YAAQyEvSF6Jb+4iWAQAAmzHwzBsUlfGF7fgR6CZWizaa+rUMxwQlWBtHM104GQkeQs15xBWEAH04jLaSUrlvS/B23HRIJi5rKN1BJO6WYOXa29RY01tCtK6j77681FozV2t6fxDxNNEXKdLy7MxD+S7gbM9OyQoAAVdmIR0/evFNONH7+0AesNTUDKPGUoE3SXOwnK6Q+4MeTPt4UlIQ/cbV3xqPGmmMZ7HF4U8ceBbZXQ/vewAZ63056ojnbnCaI+6vDGr2PkkdrC180tJYi4wGIWFQ+A5w9vFdS9NMJMheTnvS/obXJdtME4ofiYc9k9kaBpFddXOLvX+M4mO5v8lHeasr8Gc3NjeDCNzeaSVqchS2b+wKjbN0uMpEO4yZCsF0iQrz6znnbryqyA/P; NIMNR_PRODTYPE=6e5f4dd7b4604e52b8b7a6e0c72d13251b1f8731bebcc731; NIMNR_LTS=6162f3f210593a910d26aecd9b95c62626932d9bac02934e85091bc7442fa0da5fbfee560b95752f3dac5c76f8eb421c; rm_new=true; MNR_AT=qF0EgYigrlzHVMLJCzsK57Cfujj7NWMpYynylj2c; MNR_RT=rLUa6dxAnjFAXgZLiCMQeZ2JzSdqetwLHqXNlVg3; IsLoggedIn=1; NIMNR_CON=qF0EgYigrlzHVMLJCzsK57Cfujj7NWMpYynylj2c; NIMNR_TM=1747196116313; NIMNR_COMPANY=76fcef01357d7e9f677463f83be3c5547536016f85658201c7f65a9a7a3c16f5; NIMNR_USER=0efc2ec988de0f423de9a3be4ba7fd91135800ce473613f9bb04be059ae9c8570b0020cd0bfb0b8503d854324006f94c; NIMNR_UID=fc2b5ea98929ddcedb53577fcadd2eb4526ead5cd594b94f; NIMNR_CMID=5c6ddcfe91d318d3c3e674c2e386e61176e29829c0ac9cca; show_mjr=true; NIMNR_INDUS=137ab16308803e888d1e5ef1db3bbe7cc6cce18380e2c01f8e636764ae59351a6d68c9ebc0967ce9; NIMNR_TLBR=expded; NIMNR_TLBRNOTIFY=show; ng_jp=new; NIMNR_WELCOME_BACK=31aac91ba189560f893f27c7ca3bd0a0cb23d41b36e8cdb28941a1a542044b67d02bab66c1994f755fa60fb2b5e1934b2df775486112ebefa98256e771fada608182f824a0a8639f9d357d85dd015b91135800ce473613f9bb04be059ae9c8570b0020cd0bfb0b8530c6d43a918a544329420fe9a86d6d1d59b87f590c263b476d8fa4c068a2c9ce5c516a052716a7232146baa1f3edccd8d0c1d8e4a27e745ab5316d471f7d1adf303c7db60232bf65f67fbe7c972c3f8b65bade4ae126ca57d88c57918a0f9b91f9db0524023e14e2b6694664abb07eb08a14ef08ba17f641239635b60563c31b86e79309c49fce56782d19f0df0cb362f6173114eaf0a87c80d62903c24b95b67682c4f788c09e3618fd7293d2cffcd07b6921e8e8313e0f7806c4244cf0fd44a824e12cea06c8f0b59c2eae86a2947c54feb5d36ef363f04eecc3eecad45b7840924d9721b5481b83079e9e3811591cf810f390ac33bdc701f37798d8fe31b1e01f2528ebcc4f619c17dd5499f0cb5dee30337c470ccea5e7b6f322c271733f44b30cf2c9084682b3fbd7ca94e8fc5dabb134010d0a2d6b0f2ade8f75046bb616763ed267cc4bb57d73f59bf75edb0cac8c7a9bf2fba91950c760d447a91708ac391a6e2b5ef38dbabc2f0aefd659520dd6e0847db85a90977f4a468379d0078a94db72858a335308fb946b33439fac; bm_mi=9050F8777408B63A8F9E210C52A43182~YAAQyEvSF2gf/IiWAQAA7v0AzRuLqOpCYZk0ECOgDjoirwqfGnjtm4fcQNpEFOFeEDp2lYAx9sn3lzT8hBvCuFzBuId/TEIpRf1B1PPXjW3YiA3JEZ8Iyf/l5w8NORpF8AH4UX19Dwqzcqc4yxQJvi1u6+9NdBH4LQ2DoMAZjH4++PACbJSFsUG6juVqRGj8V8ZXYG3B8C15WRGATqTRAkQXe1/Q6Am3jP/m9B09jwCnSgU4SM8W8+h1ZlObSILQubvOWCJLAJTxdhIfi9qVgkvZfGrJrShOGUHvdEmIe2Mws8x0+7GRCpDmR3JmQca51Qppv2mxntUCX5xdxsojyA==~1; vt=713200093be1d1c356524833d0189bfbd422412b828b33bf9f8da26a790ca059dc538cdab905f7ea6997add858748e25a88b06342c96a2ec0ae6ca520b9e2ed89bd14949bb2aab8f7c71b3a5ba1d386bd41300b76680ad83c12dac2e7263733b95d54c1716bcb0416fcbc9491d931deea791482d1b541319546f1e395d4b32092d64036302545c7f136d6015753eee15307031d360a402169fce3c5e1ff9e8120b8429b6bd17b68aa260cc9dcdce03c5e88cfbe4057faba0b970e3b524a1ce98; JSESSIONID=EB95747E84B5E6E821D9A886F209870E; bm_sv=01C9019DA0CD5B650768D5F1A4790F1B~YAAQyEvSF/sj/IiWAQAA0GMBzRvtf5k4EX9xdAVGXO5vH8Ugy9fN4VWr7ycT+jYprXUoI2sBpMqIC3G/Mw85fmLkAgN5X3JhMzjHgJiypdpzKReqRKo+X4TAua9aFAxRL00ACqFGvMmj0GmYspDW5J1CQ+twp+9kgcteEWB0GwiajXnXaCQB6PHM1ONl5IdXsne1BR41gzw75NzYT4b2P5WzK8JG5Vo0KOCJStz8AsqTOSe6NWQDUn9P56IE3BQME4jiw1w=~1',
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
