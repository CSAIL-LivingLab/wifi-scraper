# wifi-scraper

A RESTful wifi scraper. Uploads data to the datahub.csail.mit.edu website.

To run:
ssh arcarter@saw.csail.mit.edu
cd wifi-scraper
nohup python /home/arcarter/wifi-scraper/wifi-scraper.py >> ./data/sterr.txt 2>&1 &

