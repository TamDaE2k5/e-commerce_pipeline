catalogs_ = {
    'do-tet':'Đồ tết',
    'phu-kien-tet':'Phụ kiện tết',
    'dien-thoai':'Điện thoại',
    'may-tinh-bang':'Máy tính bảng',
    'thoi-trang-nu':'Thời trang nữ',
    'thoi-trang-nam':'Thời trang nam',
    'may-anh':'Máy ảnh',
    'lam-dep-suc-khoe':'Mỹ phẩm',
    'phu-kien-thoi-trang':'Phụ kiện',
    'dong-ho-va-trang-suc':'Đồng hồ - Trang sức',
    'thiet-bi-kts':'Thiết bị kỹ thuật số',
    'phu-kien-so':'Phụ kiện số',
    'nha-sach':'Sách',
    'nha-cua-doi-song':'Nội thất',
    'do-choi-me-be':'Đồ chơi',
    'dien-gia-dung':'Điện gia dụng',
    'laptop-may-vi-tinh-linh-kien':'Laptop',
    'giay-dep-nam':'Giày - dép',
}

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.edge.options import Options
import time
from datetime import datetime
import os
import json
import random

def crawl_lazada():
    now = datetime.now()
    day = now.day
    month = now.month
    year = now.year
    hour = now.hour
    minute = now.minute
    second = now.second
    options = Options()
    options.add_argument("--disable-cache")
    options.add_argument("--disk-cache-size=0")
    options.add_argument("--incognito")
    res = []
    try:
        for kw in catalogs_.values():
            driver = webdriver.Edge(options=options)
            driver.get("https://www.lazada.vn")
            print(f'Crawl {kw}')
            # nhập tìm kiếm
            box = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "q"))
            )
            box.send_keys(kw, Keys.ENTER)

            # cào 5 trang lấy ví dụ
            for p in range(1,4):
                if p == 1:
                    # chỉ cần đợi dom product
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-qa-locator="product-item"]'))
                    )
                    products = driver.find_elements(By.CSS_SELECTOR, 'div[data-qa-locator="product-item"]')
                else:
                    # lúc này cần phải chờ dom thay đổi ví dụ dùng staless của selenium suport
                    # b1 chờ dom page list
                    next_page = WebDriverWait(driver, 15).until(
                        EC.element_to_be_clickable(
                            (By.CSS_SELECTOR, f"li.ant-pagination-item-{p}"))
                    )
                    # b2 lấy dom cũ (đổi lên b1 cx đc)
                    old_products = driver.find_elements(
                        By.CSS_SELECTOR, 'div[data-qa-locator="product-item"]'
                    )
                    # b3 cuộn xuống cho lazada load để click vào next page
                    # cái argument kia là đặt next_page làm đối số. cái này sẽ có 1 số hàm như scroll, click...
                    driver.execute_script(
                        "arguments[0].scrollIntoView({block:'center'});", next_page
                    )
                    time.sleep(random.uniform(1.5, 3.0))

                    driver.execute_script("arguments[0].click();", next_page)

                    # b4: CHỜ DATA MỚI
                    WebDriverWait(driver, 20).until(
                        EC.staleness_of(old_products[0])
                    )

                    products = WebDriverWait(driver, 20).until(
                        EC.presence_of_all_elements_located(
                            (By.CSS_SELECTOR, 'div[data-qa-locator="product-item"]')
                        )
                    )
            # đợi product load

                results = []
                for p in products:
                    try:
                        name = p.find_element(By.CLASS_NAME, "RfADt").text
                    except:
                        name = ""

                    try:
                        price = p.find_element(By.CLASS_NAME, "ooOxS").text
                    except:
                        price = ""

                    try:
                        discount = p.find_element(By.CLASS_NAME, "IcOsH").text
                    except:
                        discount = ""

                    try:
                        sold = p.find_element(By.CLASS_NAME, "_1cEkb").text
                    except:
                        sold = ""

                    try:
                        origin = p.find_element(By.CLASS_NAME, "oa6ri").text
                    except:
                        origin = ""

                    results.append({
                        'Src':'Lazada',
                        "category": kw,
                        "name": name,
                        "price": price,
                        "discount": discount,
                        "sold": sold,
                        "origin": origin,
                        'crawl-timestamp' : f'{hour}:{minute}:{second}',
                        'crawl-day': f'{day}-{month}-{year}'
                    })
                res.extend(results)
                time.sleep(2)
            driver.quit()
    except Exception as e:
        print('crawl error', e)
    
    finally:
        folder = f"bronze/{day}-{month}-{year}"
        os.makedirs(folder, exist_ok=True)

        file_path = os.path.join(folder, f"lazada_data-{hour}H.json")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(
                res,
                f,
                ensure_ascii=False,
                indent=2        # format đẹp
            )

if __name__ == '__main__':
    crawl_lazada()