import pandas as pd
import requests
import time
import random
import json
from tqdm import tqdm

cookies = {
    'TIKI_GUEST_TOKEN': '2pAUuVx7Hj9FTdQfRqeDXE0IGMa6rPky',
    'TOKENS': '{%22access_token%22:%222pAUuVx7Hj9FTdQfRqeDXE0IGMa6rPky%22%2C%22expires_in%22:157680000%2C%22expires_at%22:1896603740631%2C%22guest_token%22:%222pAUuVx7Hj9FTdQfRqeDXE0IGMa6rPky%22}',
    'amp_99d374': 'IJkc981J_Ay0zDNYya6J7Q...1ijjf9cor.1ijjfc4u0.1v.2i.4h',
    'amp_99d374_tiki.vn': 'eSc-_0HT1um7cb57E7dwA0...1enloc6a2.1enlocds8.0.1.1',
    '_gcl_au': '1.1.1465213807.1738923751',
    '_ants_utm_v2': '',
    '_trackity': '67cad4eb-5251-9d80-f45e-cbdda5fc85c5',
    '_ga_S9GLR1RQFJ':'GS1.1.1739041584.6.1.1739041673.60.0.0',
    '__iid': '749',
    '__su': '0',
    '_fbp': 'fb.1.1738923751740.928696559355059986',
    '_hjFirstSeen': '1',
    '_hjIncludedInPageviewSample': '1',
    '_hjAbsoluteSessionInProgress': '0',
    '_hjIncludedInSessionSample': '1',
    'tiki_client_id': '1246880114.1738923747',
    '_gat': '1',
    'cto_bundle': 'TrpEpF9SeVFnWTlqcHN2aGlObDVZbWphWlU4aHplNUhQdjZDR1E3RWtpbUNSTk9qc0tDbyUyQkM3cjRyVjE4dEpLc3J2WVJUU21oOCUyRkRLMDdDZlB0T25FSzhiVEolMkJRcWxKdlMzdWNTaHRGQ2dqMThGcjlBaDUzUndhTWttcmpSVWg3aDNKYkdGZ05yeE5LdElqazRNNEVSa3FTWGclM0QlM0Q',
}

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'vi,en-US;q=0.9,en;q=0.8',
    'Referer': 'https://tiki.vn/tranh-xep-hinh-tia-sang-guong-thu-tia-sang-2035-manh-ghep-p1391347.html?spid=50415863&id=1391347',
    'x-guest-token': '2pAUuVx7Hj9FTdQfRqeDXE0IGMa6rPkyY',
    'Connection': 'keep-alive',
    'TE': 'Trailers',
}

params = (
    ('platform', 'web'),
    ('spid', 50415863),
    ('version ' ,'3'),

)


# Hàm xử lý dữ liệu sản phẩm
def parser_product(json):
    d = dict()
    d['id'] = json.get('id')
    d['sku'] = json.get('sku')
    d['short_description'] = json.get('short_description')
    d['price'] = json.get('price')
    d['list_price'] = json.get('list_price')
    d['price_usd'] = json.get('price_usd')
    d['discount'] = json.get('discount')
    d['discount_rate'] = json.get('discount_rate')
    d['review_count'] = json.get('review_count')
    d['order_count'] = json.get('order_count')
    d['inventory_status'] = json.get('inventory_status')
    d['is_visible'] = json.get('is_visible')

    # Thông tin kho hàng
    stock_item = json.get('stock_item', {})
    d['stock_item_qty'] = stock_item.get('qty', None)
    d['stock_item_max_sale_qty'] = stock_item.get('max_sale_qty', None)

    d['product_name'] = json.get('meta_title')

    # Thông tin thương hiệu
    brand = json.get('brand', {})
    d['brand_id'] = brand.get('id', None)
    d['brand_name'] = brand.get('name', None)

    return d

df_id = pd.read_csv(r'C:\Users\ACER\OneDrive\Máy tính\craw_1\products-0-200000(in).csv')
p_ids = df_id.id.to_list()

# Danh sách chứa kết quả crawl
result = []
batch_size = 1000  
file_index = 1

for i, pid in enumerate(tqdm(p_ids, total=len(p_ids))):
    try:
        response = requests.get(f'https://tiki.vn/api/v2/products/{pid}', headers=headers, params=params, cookies=cookies)
        
        if response.status_code == 200:
            print(f'Crawl data {pid} success !!!')
            result.append(parser_product(response.json()))
        else:
            print(f'Failed to fetch product {pid} - Status code: {response.status_code}')

    except Exception as e:
        print(f'Error fetching product {pid}: {e}')
    
    time.sleep(random.uniform(1, 3))  

    # Lưu dữ liệu sau mỗi batch_size sản phẩm hoặc khi đến sản phẩm cuối cùng
    if len(result) >= batch_size or i == len(p_ids) - 1:
        file_name = f'crawled_data_part_{file_index}.json'
        with open(file_name, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=4)
        print(f'Saved {file_name} successfully!')

        result = []  
        file_index += 1  

print('Crawling completed!')