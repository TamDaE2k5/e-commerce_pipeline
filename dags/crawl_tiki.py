catalogs = {
    'nha-sach-tiki':'8322',
    'nha-cua-doi-song':'1883',
    'dien-thoai-may-tinh-bang':'1789',
    'do-choi-me-be':'2549',
    'thiet-bi-kts-phu-kien-so':'1815',
    'dien-gia-dung':'1882',
    'lam-dep-suc-khoe':'1520',
    'thoi-trang-nu':'931',
    'thoi-trang-nam':'915',
    'laptop-may-vi-tinh-linh-kien':'1846',
    'giay-dep-nam':'1686',
    'giay-dep-nu':'1703',
    'may-anh':'1801',
    'phu-kien-thoi-trang':'27498',
    'dong-ho-va-trang-suc':'8371',
}

catalogs_ = {
    'nha-sach-tiki':'Sách',
    'nha-cua-doi-song':'Nội thất',
    'dien-thoai-may-tinh-bang':'Điện thoại - Máy tính bảng',
    'do-choi-me-be':'Đồ chơi',
    'thiet-bi-kts-phu-kien-so':'Thiết bị kỹ thuật số - Phụ kiện số',
    'dien-gia-dung':'Điện gia dụng',
    'lam-dep-suc-khoe':'Mỹ phẩm',
    'thoi-trang-nu':'Thời trang nữ',
    'thoi-trang-nam':'Thời trang nam',
    'laptop-may-vi-tinh-linh-kien':'Laptop',
    'giay-dep-nam':'Giày - dép',
    'giay-dep-nu':'Giày - dép',
    'may-anh':'Máy ảnh',
    'phu-kien-thoi-trang':'Phụ kiện',
    'dong-ho-va-trang-suc':'Đồng hồ - Trang sức',
}


import requests
import time
from datetime import datetime

API_URL = "https://tiki.vn/api/personalish/v1/blocks/listings"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
}

def get_products(key, page=1, limit=40):
    
    urlKey = key
    category = catalogs[urlKey]

    params = {
        "limit": limit,
        "page": page,
        "category": category,
        "urlKey": urlKey,
    }

    r = requests.get(API_URL, params=params, headers=HEADERS, timeout=15)
    r.raise_for_status()
    r = r.json()
    r['Src'] = 'Tiki'
    r['category'] = catalogs_[key]
    return r

def crawl_tiki():
    now = datetime.now()
    day = now.day
    month = now.month
    year = now.year
    hour = now.hour
    minute = now.minute
    second = now.second

    all_products = []
    for catalog in catalogs:
        print(f"Crawl: {catalogs_[catalog]}")
        for page in range(1, 3):
            try:
                products = get_products(catalog, page=page)
                if not products:
                    break
                products['crawl-timestamp'] = f'{hour}:{minute}:{second}'
                products['crawl-day'] = f'{day}-{month}-{year}'
                all_products.append(products)

                time.sleep(1)
            except Exception as e:
                print("error: ", e)
                break
    
    import os
    import json

    folder = f"bronze/{day}-{month}-{year}"
    os.makedirs(folder, exist_ok=True)

    file_path = os.path.join(folder, f"tiki_data-{hour}H.json")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(
            all_products,
            f,
            ensure_ascii=False,
            indent=2        # format đẹp
        )


if __name__ == "__main__":
    crawl_tiki()