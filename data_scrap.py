import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import chromedriver_autoinstaller
from datetime import datetime
import time

chrome_options = Options()
options = [
    "--disable-gpu",
    "--window-size=1920,1200",
    "--ignore-certificate-errors",
    "--disable-extensions",
    "--no-sandbox",
    "--disable-dev-shm-usage"
]
for option in options:
    chrome_options.add_argument(option)

chromedriver_autoinstaller.install()

driver = webdriver.Chrome(options=chrome_options)

CSV_PATH = 'chennai.csv'

driver.get("https://www.makemytrip.com/hotels/hotel-listing/?checkin=03222024&city=CTMAA&checkout=03232024&roomStayQualifier=2e0e&locusId=CTMAA&country=IN&locusType=city&searchText=Chennai&regionNearByExp=3&rsc=1e2e0e")

time.sleep(5)

df = pd.DataFrame(columns=["Hotel Name", "Rating", "Rating Description", "Reviews", 
                           "Star Rating", "Location", "Nearest Landmark", "Distance to Landmark",
                           "Price", "Tax"])

print("Starting the extraction process....")
time.sleep(5)

current_time = datetime.now()
print("Extraction started at: ",current_time)

for i in range(0,1000):
    print("hotel: "+str(i))
    try:
        content = driver.find_element(By.XPATH,'//*[@id="Listing_hotel_'+str(i)+'"]')
    except:
         print("End of Hotels")
         break
    hname = content.find_element(By.ID,'hlistpg_hotel_name')
    try:
        rating = content.find_element(By.ID,'hlistpg_hotel_user_rating')
        rating = rating.text
        try:
            rating_desc = content.find_element(By.XPATH,'//*[@id="Listing_hotel_'+str(i)+'"]/a/div/div[1]/div[2]/div[1]/div/div/span[1]')
            rating_desc = rating_desc.text
        except:
            rating_desc = content.find_element(By.XPATH,'//*[@id="Listing_hotel_'+str(i)+'"]/a/div/div/div[1]/div[2]/div[2]/div/div/span[2]')
            rating_desc = rating_desc.text
        review_count = content.find_element(By.ID,'hlistpg_hotel_reviews_count')
        review_count = review_count.text
    except:
        rating=""
        rating_desc=""
        review_count=""
    
    loc = content.find_element(By.CLASS_NAME,'pc__html')
    loc = loc.text
    loc = loc.split("|")
    location = loc[0] 
    try:
        landmark = loc[1].split('from')
        dist_landmark = landmark[0].lstrip() 
        landmark = landmark[1].lstrip() 
    except:
        dist_landmark=""
        landmark=""
    price = content.find_element(By.ID,'hlistpg_hotel_shown_price')
    
    tax = content.find_element(By.XPATH,'//*[@id="Listing_hotel_'+str(i)+'"]/a/div[1]/div/div[2]/div/div/p[2]')
    try:
        tax = tax.text.split(" ")[2]
    except:
        tax=""
    try:
        s_rating = content.find_element(By.ID,'hlistpg_hotel_star_rating')
        s_rating = s_rating.get_attribute('data-content')
    except:
        s_rating=""

    data=[hname.text,rating,rating_desc,review_count,s_rating,location,landmark,dist_landmark,price.text[2:],tax]
    row_dict = dict(zip(df.columns, data))
    row_df = pd.DataFrame([row_dict])  
    df = pd.concat([df, row_df], ignore_index=True)
    time.sleep(2)
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

time.sleep(5)
print("extraction done...file saved")
time.sleep(2)
end_time = datetime.now()
print("Extraction ended at: ",end_time)
print("---------------------------------------")
time_difference = end_time - current_time
total_seconds = time_difference.total_seconds()
total_minutes = total_seconds // 60
remaining_seconds = total_seconds % 60
print("Time taken for extraction:", total_minutes, "minutes and", remaining_seconds, "seconds")

df.to_csv("hotel_dataset.csv",index=False)
driver.close()

