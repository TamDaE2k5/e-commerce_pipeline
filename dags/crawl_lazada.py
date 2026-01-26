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

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from datetime import datetime
import os
import json

def crawl_lazada():
    now = datetime.now()
    day = now.day
    month = now.month
    year = now.year
    hour = now.hour
    minute = now.minute
    second = now.second

    res = []
    for kw in catalogs_.values():
        driver = webdriver.Edge()
        driver.get("https://www.lazada.vn")
        print(f'Crawl {kw}')
        # nhập tìm kiếm
        box = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "q"))
        )
        box.send_keys(kw, Keys.ENTER)

        # đợi product load
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div[data-qa-locator="product-item"]'))
        )

        products = driver.find_elements(By.CSS_SELECTOR, 'div[data-qa-locator="product-item"]')

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
        driver.quit()

    folder = f"bronze/{day}-{month}-{year}"
    os.makedirs(folder, exist_ok=True)

    file_path = os.path.join(folder, f"lazada_data-{hour}H.json")

    with open(file_path, "a", encoding="utf-8") as f:
        json.dump(
            res,
            f,
            ensure_ascii=False,
            indent=2        # format đẹp
        )

if __name__ == '__main__':
    crawl_lazada()