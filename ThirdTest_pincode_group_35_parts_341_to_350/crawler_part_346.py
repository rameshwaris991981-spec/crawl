"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 346 / 400
================================================================================
- Group: ThirdTest_pincode_group_35_parts_341_to_350
- Assigned PIN Codes: 48 (Range: 758025 to 759028)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_346.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_346.csv & .json
- Concurrency: 16 Workers (High-throughput & resilient)
================================================================================
"""

import os
import sys
import re
import csv
import time
import json
import random
import logging
import urllib.parse
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PART_ID = "part_346"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-346] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "758025",
  "758026",
  "758027",
  "758028",
  "758029",
  "758030",
  "758031",
  "758032",
  "758034",
  "758035",
  "758036",
  "758037",
  "758038",
  "758040",
  "758041",
  "758043",
  "758044",
  "758045",
  "758046",
  "758047",
  "758076",
  "758078",
  "758079",
  "758080",
  "758081",
  "758082",
  "758083",
  "758084",
  "758085",
  "758086",
  "758087",
  "759001",
  "759013",
  "759014",
  "759015",
  "759016",
  "759017",
  "759018",
  "759019",
  "759020",
  "759021",
  "759022",
  "759023",
  "759024",
  "759025",
  "759026",
  "759027",
  "759028"
]

# 256 Unique Business Categories
CATEGORIES = [
  "Kirana Store",
  "Supermarket",
  "Departmental Store",
  "Provision Store",
  "Organic Food Store",
  "Dairy and Milk Parlour",
  "Fruit and Vegetable Wholesaler",
  "Dry Fruits and Spices Wholesaler",
  "Flour Mill",
  "Edible Oil Wholesaler",
  "Rice and Grain Merchant",
  "Meat and Poultry Shop",
  "Fish Market",
  "General Store",
  "Paan and FMCG Stall",
  "FMCG Distributor",
  "Frozen Food Distributor",
  "Pet Food and Pet Supplies",
  "Sweet Stall / Mithai Shop",
  "Bakery and Cake Shop",
  "Patisserie",
  "Tea Stall / Chai Cafe",
  "Juice Center and Milkshake Bar",
  "Pure Veg Restaurant",
  "Non-Veg Biryani Restaurant",
  "Dhaba and Highway Restaurant",
  "Tiffin Center and Mess",
  "South Indian Restaurant",
  "North Indian Restaurant",
  "Fast Food and Chaat Corner",
  "Cloud Kitchen",
  "Cafe and Coffee Shop",
  "Ice Cream Parlour",
  "Bar and Pub",
  "Family Restaurant",
  "Restaurant Chains",
  "Saree Showroom",
  "Silk Saree Wholesaler",
  "Readymade Garments Shop",
  "Mens Wear Showroom",
  "Womens Ethnic Wear and Kurti",
  "Kids Wear Store",
  "Tailor and Fashion Designer",
  "Textile Wholesaler and Fabric Merchant",
  "Gold and Diamond Jewellery Showroom",
  "Silver Jewellery Shop",
  "Goldsmith and Jewellery Repair",
  "Artificial Jewellery and Accessories",
  "Footwear and Shoe Store",
  "Leather Goods and Bags",
  "Handloom and Khadi Store",
  "Uniform Manufacturer",
  "Bridal Wear and Wedding Collection",
  "Hosiery and Undergarments Wholesaler",
  "Watch Showroom and Repair",
  "Optical Store and Eyewear",
  "Boutiques",
  "Luxury Clothing Shops",
  "Medical Store / Pharmacy",
  "24 Hour Pharmacy",
  "Ayurvedic Pharmacy and Clinic",
  "Homeopathic Clinic",
  "Multispeciality Hospital",
  "Nursing Home and Maternity Hospital",
  "Clinics",
  "Doctors",
  "Dental Clinic",
  "Eye Clinic and Eye Hospital",
  "Skin Clinic and Dermatologist",
  "Pediatrician and Child Clinic",
  "Orthopedic and Physiotherapy Clinic",
  "Diagnostic Center",
  "Pathology Lab and Blood Test",
  "Polyclinic",
  "Dialysis Center",
  "ENT Clinic",
  "Veterinary Clinic and Pet Hospital",
  "Surgical Equipment Supplier",
  "Medical Equipment Supplier",
  "Yoga Center",
  "Gym and Fitness Center",
  "Fitness Chains",
  "Healthcare Clinic Chains",
  "Two Wheeler Repair and Mechanic",
  "Car Repair Workshop and Garage",
  "Car Wash and Auto Detailing",
  "Two Wheeler Showroom and Dealer",
  "Car Showroom and Used Car Dealer",
  "Commercial Vehicle and Tractor Dealer",
  "Auto Spare Parts Shop",
  "Tyre Showroom and Puncture Shop",
  "Car and Bike Battery Dealer",
  "Auto Electrician and AC Repair",
  "CNG Kit Fitment Center",
  "Bicycle Shop and Repair",
  "Taxi Service and Car Rental",
  "Tour and Travel Operator",
  "Bus Booking Agency",
  "Packers and Movers",
  "Logistics and Transport Services",
  "Tempo and Mini Truck Service",
  "Crane and Towing Service",
  "Driving School",
  "Automotive Service Chains",
  "Hardware Store",
  "Electrical Goods and Lighting Store",
  "Sanitaryware and Bathroom Fittings",
  "Paint and Putty Dealer",
  "Tile and Marble Showroom",
  "Granite Dealer",
  "Plywood and Timber Merchant",
  "Glass and Mirror Merchant",
  "Cement and Sand Supplier",
  "TMT Steel and Iron Wholesaler",
  "Building Material Supplier",
  "Borewell Drilling Contractor",
  "Plumber",
  "Electrician",
  "AC Fridge and Washing Machine Repair",
  "RO Water Purifier Sales and Service",
  "Solar Rooftop and Inverter Dealer",
  "Interior Designers",
  "Architects",
  "Civil Contractor and Builder",
  "Roofing Sheet Supplier",
  "False Ceiling Contractor",
  "Waterproofing Contractor",
  "Modular Kitchen Manufacturer",
  "Furniture Showroom",
  "Salon",
  "Beauty Parlour",
  "Spa",
  "Unisex Salon",
  "Bridal Makeup Artist",
  "Cosmetics Wholesaler",
  "Tattoo and Nail Art Studio",
  "Herbal and Ayurvedic Cosmetic Products",
  "Hair Transplant Clinic",
  "Spa Equipment Suppliers",
  "Spa Consultants",
  "Wellness Center",
  "Therapy Center",
  "Marriage Hall / Kalyana Mandapam",
  "Banquet Hall",
  "Event Planners/Wedding Planners",
  "Flower Decorator",
  "Balloon Decorator",
  "Tent House and Shamiana",
  "Sound and Light Rental",
  "Caterer and Event Planner",
  "Photographers",
  "Videographer and Drone Rental",
  "Hotel",
  "Resort",
  "Hostels",
  "PG",
  "Guesthouse",
  "Trousseau Home Decor",
  "Gifting",
  "Cleaning and Hotel Supplier shops/ wholesalers",
  "Hotel Kit Suppliers",
  "Hospitality Consultants",
  "Media and Event",
  "Corporate Event Planner",
  "School",
  "Play School and Daycare",
  "Junior College and Degree College",
  "NEET and JEE Coaching Center",
  "Commerce and CA Coaching",
  "Spoken English Institute",
  "Computer Training Institute",
  "Competitive Exam Coaching (UPSC/Banking)",
  "Tuition Center",
  "Music and Dance Academy",
  "Sports Academy and Turf Ground",
  "Bookstore and Stationery Shop",
  "Educational Consultant",
  "Xerox and Photostat Center",
  "Printing Press and Offset Printer",
  "Flex and Banner Printing",
  "Wedding Invitation Card Printer",
  "Common Service Center (CSC) / E-Seva",
  "Internet Cafe",
  "Computer Sales and Laptop Repair",
  "CCTV Installation and Security System",
  "Mobile Phone Sales and Repair",
  "Mobile Accessories Wholesaler",
  "POS and Billing Software Vendor",
  "Document Writer and Stamp Vendor",
  "IT and Telecom Services",
  "Chartered Accountant (CA)",
  "Tax and GST Consultant",
  "Advocate and Lawyer",
  "Insurance Agent",
  "Home Loan DSA and Loan Consultant",
  "Money Transfer and Forex",
  "Microfinance and NBFC",
  "Pawn Broker and Gold Loan",
  "Chit Fund Company",
  "Stock Broker and Share Sub-broker",
  "Company Registration Consultant",
  "HR Planning and Recruitment",
  "Courier and Cargo Service",
  "Security Guard Agency",
  "Housekeeping Services",
  "Scrap Dealer and Raddi Wholesaler",
  "Financial and Legal Services",
  "Business and Audit Services",
  "Real Estate Agents",
  "Commercial Real Estate Brokerages",
  "Premium Luxury Real Estate",
  "Property Developers",
  "Steel Fabrication Workshop",
  "Welding and Lathe Works",
  "CNC Machining and Laser Cutting",
  "Aluminium Fabrication",
  "Plastic Molding Manufacturer",
  "Corrugated Box and Packaging Material Manufacturers",
  "Chemical Wholesalers",
  "Industrial Hardware and Fasteners",
  "Motor Rewinding and Pump Repair",
  "Generator Sales and Rental",
  "Warehouse and Cold Storage",
  "Rice Mill and Agro Processing",
  "Flour and Oil Mill",
  "Fertilizer and Pesticide Dealer",
  "Agricultural Machinery and Harvester",
  "Industrial Equipment Suppliers",
  "Importers",
  "Exporters",
  "EXIMS",
  "Tradeshows",
  "Exhibitions",
  "Digital Marketing Agencies",
  "Local SEO Agencies",
  "SEO Agencies",
  "SEO Consultants",
  "PPC Advertising Agencies",
  "Social Media Marketing Agencies",
  "Advertisement Agency",
  "Growth Marketing",
  "Lead Generation Agencies",
  "B2B Appointment-Setting Agencies",
  "Telemarketing Firms",
  "SaaS Companies Selling to SMBs",
  "CRM Data Enrichment Companies",
  "Market Research Firms",
  "Malls",
  "Shopping Mall Operators",
  "Multi-location Retail Chains",
  "Commercial Complex",
  "Wholesale Market / Mandi",
  "Industrial Estate / GIDC / MIDC / SIPCOT",
  "Shops",
  "Offices",
  "Businesses"
]

# Pincode to City/Region/Circle Metadata Map
PINCODE_METADATA = {
  "758025": {
    "pincode": "758025",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Kushaleswar S.O",
      "Balarampur B.O",
      "Balipal B.O",
      "Bangarkota B.O",
      "Baripal B.O",
      "Bhandaridiha B.O",
      "Daradipal B.O",
      "Jalasuanpatna B.O",
      "Kanpur B.O",
      "Keshudurapal B.O",
      "Khalapal B.O",
      "Kochianandi B.O",
      "Radhikadeipur B.O",
      "Samukanandi B.O",
      "Tarimul B.O",
      "Toraniapal B.O",
      "Uchaabali B.O"
    ]
  },
  "758026": {
    "pincode": "758026",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Talapada S.O",
      "Baliparbata B.O"
    ]
  },
  "758027": {
    "pincode": "758027",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Ghatagaon S.O",
      "Badajamposi B.O",
      "Baiganpal B.O",
      "Balipokhari B.O",
      "Bandakanda Haladharpur B.O",
      "Barahatipura B.O",
      "Dhangadadiha B.O",
      "Gadadharpur B.O",
      "Kendudiha B.O",
      "Kundapitha B.O",
      "Kusunpur B.O",
      "Manata B.O",
      "Mukundapur Patna B.O",
      "Nusuriposi B.O",
      "Rajabandha B.O",
      "Tara B.O"
    ]
  },
  "758028": {
    "pincode": "758028",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Harichandanpur S.O",
      "Badanuagaon B.O",
      "Badasialimal B.O",
      "Bhawanarpur B.O",
      "Chakradharpur B.O",
      "Gandadiha B.O",
      "Hundapalasapal B.O",
      "Jiranga B.O",
      "Kaduadiha B.O",
      "Kuntla B.O",
      "Pitapiti B.O",
      "Pithagola B.O",
      "Sunapentha B.O",
      "Tambahara B.O",
      "Thakurapada B.O"
    ]
  },
  "758029": {
    "pincode": "758029",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Dhenkikote S.O",
      "Bholabeda B.O",
      "Chinamaliposi B.O",
      "Kathabari B.O",
      "Katrabeda B.O",
      "Ketanga B.O",
      "Manoharpur B.O",
      "Muktapur B.O",
      "Palanghati B.O",
      "Pipilia B.O",
      "Poipani B.O",
      "Sanamasinabila B.O",
      "Santrapur B.O",
      "Toranipokhari B.O"
    ]
  },
  "758030": {
    "pincode": "758030",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Swampatna S.O",
      "Brahmanideo B.O",
      "Dumuria B.O",
      "Erendei B.O",
      "Gobarbeda B.O",
      "Mushakhari B.O",
      "Saraskela B.O",
      "Tangarpada B.O"
    ]
  },
  "758031": {
    "pincode": "758031",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Jhumpura S.O",
      "Chipinda B.O",
      "Handibhanga B.O",
      "Kandraposi B.O",
      "Kanjipani B.O",
      "Kasipal B.O",
      "Kathabaunsuli B.O",
      "Kaunrikala(B) B.O",
      "Khendra B.O",
      "Khuntapada B.O",
      "Malda B.O",
      "Nahabeda B.O",
      "Palasapanga B.O",
      "Parajanpur B.O",
      "Raikala B.O",
      "Silisuan B.O",
      "Arsala B.O",
      "Balibandha B.O"
    ]
  },
  "758032": {
    "pincode": "758032",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Ukhunda S.O",
      "Asanpat B.O",
      "Badadumuria B.O",
      "Badaneuli B.O",
      "Baria B.O",
      "Baunsuli B.O",
      "Bhodaposi B.O",
      "Bhuinpur B.O",
      "Bhulda B.O",
      "Gidhibas B.O",
      "Jally B.O",
      "Kaunrikala(A) B.O",
      "Niundi B.O",
      "Padua B.O",
      "Radhikadeipur B.O",
      "Rasabantala B.O",
      "Sarasinga B.O",
      "Silitia B.O",
      "Tukudiha B.O"
    ]
  },
  "758034": {
    "pincode": "758034",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Joda S.O",
      "Bansapani B.O",
      "Bichakundi B.O",
      "Gurutuan B.O",
      "Jajanga B.O",
      "Khandabandha B.O"
    ]
  },
  "758035": {
    "pincode": "758035",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Barbil S.O",
      "Bhadrasahi B.O",
      "Bhuyanroida B.O",
      "Guali B.O",
      "Kasia(KA) B.O",
      "Nalda B.O",
      "Serenda B.O",
      "Thakurani B.O",
      "Sundara Shiv Mandir S.O"
    ]
  },
  "758036": {
    "pincode": "758036",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Matkambeda S.O",
      "Kolhabarapada B.O"
    ]
  },
  "758037": {
    "pincode": "758037",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Bolani S.O",
      "Balagoda B.O"
    ]
  },
  "758038": {
    "pincode": "758038",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Baneikala S.O",
      "Bileipada B.O",
      "Birikala B.O",
      "Daduan B.O",
      "Deojhar B.O",
      "Kolhahundula B.O"
    ]
  },
  "758040": {
    "pincode": "758040",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Kiriburu Hill Top S.O",
      "Dhanurjaypur Haramatha B.O"
    ]
  },
  "758041": {
    "pincode": "758041",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Champua S.O",
      "Chamakpur B.O",
      "Dhobakuchuda B.O",
      "Jagannathpur Ashram B.O",
      "Jamudalak B.O",
      "Kainta B.O",
      "Kadagadia B.O",
      "Panchapokharia B.O",
      "Sarei B.O",
      "Sasanga B.O",
      "Sunaposi B.O"
    ]
  },
  "758043": {
    "pincode": "758043",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Sainkul S.O",
      "Alati B.O",
      "Batto B.O",
      "Bhaganai B.O",
      "Bhaluka B.O",
      "Garabandagoda B.O",
      "Jalasuan B.O",
      "Kendukhunta B.O",
      "Khalana B.O",
      "Machhala B.O",
      "Madanpur B.O",
      "Madhukeshari B.O",
      "Nandabara B.O",
      "Pandua B.O",
      "Patsura B.O",
      "Purunabandagoda B.O",
      "Ramachandrapur B.O",
      "Taruan B.O"
    ]
  },
  "758044": {
    "pincode": "758044",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Karanjia S.O (Kendujhar)",
      "Badanai B.O",
      "Bankia B.O",
      "Bhanda B.O",
      "Chauthia B.O",
      "Gundunia B.O",
      "Kalinga B.O",
      "Kanjiasula B.O",
      "Kasipal B.O",
      "Nandapur B.O",
      "Padmapur B.O",
      "Pokharia B.O",
      "Rangamatia B.O",
      "Raruanguda B.O",
      "Sologuda B.O",
      "Uchhabali B.O"
    ]
  },
  "758045": {
    "pincode": "758045",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Udayapur S.O",
      "Baikala B.O",
      "Jamuda B.O",
      "Khajirapat B.O",
      "Khuntapada B.O",
      "Khuntapingu B.O",
      "Malarpada B.O",
      "Silipada B.O"
    ]
  },
  "758046": {
    "pincode": "758046",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Turumunga S.O",
      "Budhikapudi B.O",
      "Chemana B.O",
      "Childa B.O",
      "Jyotipur B.O",
      "Kankada B.O",
      "Khireitangiri B.O",
      "Phulkanlei B.O",
      "Sadangi B.O",
      "Putugaon B.O"
    ]
  },
  "758047": {
    "pincode": "758047",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Remuli S.O",
      "Amalanigoda B.O",
      "Anseikala B.O",
      "Balabhadrapur B.O",
      "Basira B.O",
      "Basudevpur B.O",
      "Gumura B.O",
      "Kalikaprasad B.O",
      "Kandra B.O",
      "Naradapur B.O",
      "Nayakrihsnapur B.O",
      "Nischintapur B.O",
      "Parsala B.O",
      "Rajia B.O",
      "Taduabahal B.O"
    ]
  },
  "758076": {
    "pincode": "758076",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Sirigida S.O",
      "Akula B.O",
      "Balabhadrapur B.O",
      "Bhimkand B.O",
      "Bimala B.O",
      "Birabarpur Nuagaon B.O",
      "Kaliahata B.O",
      "Karamangi B.O",
      "Nuagaon B.O",
      "Podanga B.O",
      "Purujoda B.O",
      "Raisuan Patkholi B.O",
      "Sunduria B.O"
    ]
  },
  "758078": {
    "pincode": "758078",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Dhanurjaypur S.O",
      "Barigaon B.O",
      "Bidyadharpur B.O",
      "Kahaliagadia B.O",
      "Kanpur B.O",
      "Padhiaripally B.O",
      "Raighati B.O",
      "Soso B.O"
    ]
  },
  "758079": {
    "pincode": "758079",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Janghira S.O",
      "Baxibarigaon B.O",
      "Hunda B.O",
      "Junga B.O",
      "Khajuribani B.O",
      "Masinajodi B.O",
      "Mishramala B.O",
      "Somagiri B.O",
      "Tentlaposi B.O"
    ]
  },
  "758080": {
    "pincode": "758080",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Bhagamunda S.O",
      "Billa B.O",
      "Chilikdhara B.O",
      "Karadapal B.O",
      "Masinajoda B.O",
      "Nuagaon B.O",
      "Revanapalaspal B.O",
      "Sagadapata B.O"
    ]
  },
  "758081": {
    "pincode": "758081",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Machhagarh S.O",
      "Banabir B.O",
      "Damahuda B.O",
      "Gholkunda B.O",
      "Goras B.O",
      "Khadikapada B.O",
      "Mirigikhoji B.O"
    ]
  },
  "758082": {
    "pincode": "758082",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Brahmanipal S.O",
      "Alutuma B.O",
      "Kansa B.O",
      "Tangiriapal B.O"
    ]
  },
  "758083": {
    "pincode": "758083",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Hatadihi S.O",
      "Badarampas B.O",
      "Chhenapadi B.O",
      "Inchola B.O",
      "Mareigaon B.O",
      "Orali B.O"
    ]
  },
  "758084": {
    "pincode": "758084",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Jagamohanpur S.O",
      "Binjabahal B.O",
      "Chhamunda B.O",
      "Nuadihi B.O",
      "Saleikana B.O",
      "Saruali B.O",
      "Tana B.O"
    ]
  },
  "758085": {
    "pincode": "758085",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Banspal S.O",
      "Karangadihi B.O",
      "Kushakala B.O",
      "Kadakala B.O",
      "Kumundi (KHA) B.O",
      "Nayakote B.O",
      "Baragoda B.O",
      "Jatra B.O",
      "Urumunda B.O",
      "Fuljhar B.O",
      "Taramakanta B.O",
      "Raigoda B.O",
      "Singpur B.O"
    ]
  },
  "758086": {
    "pincode": "758086",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Bamebari S.O.",
      "Belda B.O",
      "Dabuna B.O",
      "Basantapur B.O",
      "Balada B.O",
      "Chormalda B.O",
      "Silijoda B.O",
      "Guruda B.O",
      "Palasa (KHA) B.O"
    ]
  },
  "758087": {
    "pincode": "758087",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Pandapada S.O",
      "Basantapur B.O",
      "Binida B.O",
      "Badapichhula B.O",
      "Balabhadrapur B.O",
      "Jharbeda B.O",
      "Patilo B.O",
      "Purumunda B.O",
      "Rutisila B.O"
    ]
  },
  "759001": {
    "pincode": "759001",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Dhenkanal Division",
    "offices": [
      "Dhenkanal H.O",
      "Dhenkanal College S.O",
      "Jubuli Town S.O"
    ]
  },
  "759013": {
    "pincode": "759013",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Dhenkanal Division",
    "offices": [
      "Dhenkanal R S S.O",
      "Barada B.O",
      "Chaulia B.O",
      "Gengutia B.O",
      "Kankadpal B.O",
      "Korian B.O",
      "Mahisapat B.O",
      "Saptasajya B.O",
      "Shankarpur B.O",
      "Tarava B.O",
      "Bajichowk B.O"
    ]
  },
  "759014": {
    "pincode": "759014",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Dhenkanal Division",
    "offices": [
      "Mahimagadi S.O",
      "Badanagena B.O",
      "Bainsia B.O",
      "Banasingh B.O",
      "Chirulei B.O",
      "Kabera Madhapur B.O",
      "Kaluria B.O",
      "Karamul B.O",
      "Kendupada B.O",
      "Khankar B.O",
      "Mahapada B.O",
      "Neulpoi B.O",
      "Radhadeipur B.O"
    ]
  },
  "759015": {
    "pincode": "759015",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Dhenkanal Division",
    "offices": [
      "Bhapur S.O (Dhenkanal)",
      "Bhaliabolkateni B.O",
      "Chandrasekharaprasad B.O",
      "Dhirapatna B.O",
      "Ghatipiri B.O",
      "Govindprasad B.O",
      "Kakudibhag B.O",
      "Kalanga B.O",
      "Kalikaprasad B.O",
      "Kamadhenukote B.O",
      "Kankadahad Sadar B.O",
      "Kottam B.O",
      "Mangalpur B.O",
      "Sarakpatana B.O"
    ]
  },
  "759016": {
    "pincode": "759016",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Dhenkanal Division",
    "offices": [
      "Gondiapatna S.O",
      "Barada B.O",
      "Bidharpur B.O",
      "Kalunigoda B.O",
      "Kapilash B.O",
      "Kashipur B.O",
      "Khankira B.O",
      "Lauloi B.O",
      "Letheka B.O",
      "Mandar B.O",
      "Mathatentulia B.O",
      "Nihalprasad B.O",
      "Nuagarh B.O",
      "Pingua B.O",
      "Raitala B.O",
      "Sadangi B.O",
      "Santhapur B.O",
      "Sorisiapada B.O"
    ]
  },
  "759017": {
    "pincode": "759017",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Dhenkanal Division",
    "offices": [
      "Bhuban S.O",
      "Anantapur B.O",
      "Anlajhari B.O",
      "Balibo B.O",
      "Bhusal B.O",
      "Chandipal B.O",
      "Dhalapada B.O",
      "Ekatali B.O",
      "Mahulapal B.O",
      "Ramakrishnapur B.O",
      "Rendapatana B.O"
    ]
  },
  "759018": {
    "pincode": "759018",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Dhenkanal Division",
    "offices": [
      "Kamakhyanagar S.O",
      "Batagaon B.O",
      "Baunsapal B.O",
      "Bhagirathipur B.O",
      "Jayapurakateni B.O",
      "Kamarda B.O",
      "Kanapura B.O",
      "Khajuria B.O",
      "Mahulpal B.O",
      "Malapura B.O",
      "Motta B.O",
      "Muktapasi B.O",
      "Pipala B.O",
      "Samatangi B.O",
      "Saruali B.O",
      "Sibulapasi B.O"
    ]
  },
  "759019": {
    "pincode": "759019",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Dhenkanal Division",
    "offices": [
      "Hindol Road R.S. S.O",
      "Babandha B.O",
      "Balaramprasad B.O",
      "Bangursinga B.O",
      "Bedapada B.O",
      "Chandpur B.O",
      "Dhalapur B.O",
      "Kumusi B.O",
      "Nadhara B.O",
      "Saanda B.O"
    ]
  },
  "759020": {
    "pincode": "759020",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Dhenkanal Division",
    "offices": [
      "Balimi S.O",
      "Badalo B.O",
      "Chitalpur B.O",
      "Dudurkote B.O",
      "Giridharaprasad B.O",
      "Hatura B.O",
      "Kalanda B.O",
      "Karanda B.O",
      "Krishnachandrapur B.O",
      "Panchapada B.O",
      "Ranjagol B.O",
      "Thokar B.O"
    ]
  },
  "759021": {
    "pincode": "759021",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Dhenkanal Division",
    "offices": [
      "Rasol S.O",
      "Buhalipal B.O",
      "Chhotapada B.O",
      "Gandanali B.O",
      "Kalingapal B.O",
      "Khaliborei B.O",
      "Kunua B.O",
      "Nuabag B.O"
    ]
  },
  "759022": {
    "pincode": "759022",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Dhenkanal Division",
    "offices": [
      "Hindol S.O",
      "Baunsapokhari B.O",
      "Kansara B.O",
      "Maidharpur B.O",
      "Phulapada B.O"
    ]
  },
  "759023": {
    "pincode": "759023",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Dhenkanal Division",
    "offices": [
      "Marthapur S.O",
      "Bankual B.O",
      "Bhairpur B.O",
      "Dighi B.O",
      "Gobinda Bidyadharpur B.O",
      "Gorodia B.O",
      "Guneibil B.O",
      "Jineilo B.O",
      "Jiral B.O",
      "Khurusia B.O"
    ]
  },
  "759024": {
    "pincode": "759024",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Dhenkanal Division",
    "offices": [
      "Mathakargola S.O",
      "Anal B.O",
      "Baruan B.O",
      "Garh Nrusinghaprasad B.O",
      "Jamunakote B.O",
      "Odisha B.O",
      "Surapratapapur B.O"
    ]
  },
  "759025": {
    "pincode": "759025",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Dhenkanal Division",
    "offices": [
      "Gadasila S.O",
      "Balarampur B.O",
      "Bampa B.O",
      "Baulpur B.O",
      "Belapada B.O",
      "Brahmaniapal B.O",
      "Gunadei B.O",
      "Gundichapada B.O",
      "Indipur B.O",
      "Kandabindha B.O",
      "Kasiadihi B.O",
      "Sadasivapur B.O",
      "Siminai B.O"
    ]
  },
  "759026": {
    "pincode": "759026",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Dhenkanal Division",
    "offices": [
      "Anlabereni S.O",
      "Aluajharan B.O",
      "Bamuan B.O",
      "Gangijodi B.O",
      "Ichhabatipur B.O",
      "Kantapal B.O",
      "Kantiokateni B.O",
      "Kantioputasahi B.O",
      "Kotagara B.O",
      "Kusumjodi B.O",
      "Rainrusinghpur B.O",
      "Sogar B.O",
      "Tumusinga B.O"
    ]
  },
  "759027": {
    "pincode": "759027",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Dhenkanal Division",
    "offices": [
      "Govindpur S.O (Dhenkanal)",
      "Baladiabandha B.O",
      "Belatikiri B.O",
      "Gahamkhunti B.O",
      "Kaimati B.O",
      "Mahulapada B.O",
      "Manipur B.O",
      "Nadiali B.O",
      "Talabarkote B.O"
    ]
  },
  "759028": {
    "pincode": "759028",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Dhenkanal Division",
    "offices": [
      "Kankadahad S.O",
      "Bam B.O",
      "Kandhara B.O",
      "Kantapal B.O",
      "Kantol B.O",
      "Karagola B.O",
      "Kerjoli B.O",
      "Tariniposi B.O"
    ]
  }
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
]

class SplitPincodeLeadCrawler:
    def __init__(self, max_workers=16):
        self.max_workers = max_workers
        self.session = self._create_resilient_session()
        self.results = []
        self.seen_keys = set()
        self.completed_combos = set()
        self.last_git_push_count = 0
        
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.part_dir = os.path.join(self.script_dir, PART_ID)
        
        # 4 Output Directories
        self.master_dir = os.path.join(self.part_dir, "master")
        self.by_pincode_dir = os.path.join(self.part_dir, "by_pincode")
        self.by_category_dir = os.path.join(self.part_dir, "by_category")
        self.combos_dir = os.path.join(self.part_dir, "by_combination")
        self.ref_dir = os.path.join(self.part_dir, "pincode_city_reference")
        
        for d in [self.master_dir, self.by_pincode_dir, self.by_category_dir, self.combos_dir, self.ref_dir]:
            os.makedirs(d, exist_ok=True)
            
        self.checkpoint_file = os.path.join(self.part_dir, f"checkpoint_{PART_ID}.json")
        self.save_reference_metadata()
        self.load_checkpoint()

    def save_reference_metadata(self):
        try:
            ref_json = os.path.join(self.ref_dir, f"pincode_city_mapping_{PART_ID}.json")
            ref_csv = os.path.join(self.ref_dir, f"pincode_city_mapping_{PART_ID}.csv")
            with open(ref_json, 'w', encoding='utf-8') as f:
                json.dump(PINCODE_METADATA, f, indent=2, ensure_ascii=False)
            with open(ref_csv, 'w', newline='', encoding='utf-8-sig') as f:
                w = csv.DictWriter(f, fieldnames=["pincode", "circle", "region", "division", "offices"])
                w.writeheader()
                for p, meta in PINCODE_METADATA.items():
                    w.writerow({
                        "pincode": meta.get("pincode", p),
                        "circle": meta.get("circle", "N/A"),
                        "region": meta.get("region", "N/A"),
                        "division": meta.get("division", "N/A"),
                        "offices": ", ".join(meta.get("offices", []))
                    })
        except Exception as e:
            logger.warning(f"Could not save reference metadata: {e}")

    def _create_resilient_session(self):
        s = requests.Session()
        retries = Retry(total=5, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries, pool_connections=64, pool_maxsize=64)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update({
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
            "Accept": "*/*",
            "Referer": "https://www.google.com/"
        })
        return s

    def load_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.completed_combos = set(data.get("completed_combos", []))
                    logger.info(f"Loaded checkpoint: {len(self.completed_combos)} combinations already completed.")
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")

    def save_checkpoint(self):
        try:
            with open(self.checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump({"completed_combos": list(self.completed_combos), "updated_at": datetime.now().isoformat()}, f)
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")

    def _extract_phone(self, details):
        def deep_search(obj):
            if isinstance(obj, str):
                cleaned = obj.strip()
                if re.match(r"^(\+91[\-\s]?)?[0]?(91)?[6789]\d{9}$", cleaned) or (cleaned.startswith("+91") and len(cleaned) >= 13):
                    return cleaned
                if re.match(r"^0\d{2,4}[\-\s]?\d{6,8}$", cleaned):
                    return cleaned
            elif isinstance(obj, list):
                for item in obj:
                    res = deep_search(item)
                    if res:
                        return res
            elif isinstance(obj, dict):
                for v in obj.values():
                    res = deep_search(v)
                    if res:
                        return res
            return None
        found = deep_search(details)
        return found if found else "N/A"

    def _generate_search_angles(self, pincode, category):
        return [
            f"{category} in {pincode}",
            f"Best {category} in {pincode}",
            f"{category} near {pincode}",
            f"{category} dealers suppliers in {pincode}"
        ]

    def git_auto_push_milestone(self, lead_count):
        logger.info("=" * 60)
        logger.info(f"[*] AUTO-SAVE TRIGGERED: {lead_count:,} Leads Scraped! Committing to GitHub...")
        logger.info("=" * 60)
        
        self.export_all()
        self.save_checkpoint()
        
        try:
            repo_root = os.path.abspath(os.path.join(self.script_dir, ".."))
            subprocess.run(["git", "config", "user.name", "github-actions[bot]"], cwd=repo_root, capture_output=True)
            subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], cwd=repo_root, capture_output=True)
            
            rel_part = os.path.relpath(self.part_dir, repo_root)
            subprocess.run(["git", "add", "-A", rel_part], cwd=repo_root, capture_output=True)
            commit_msg = f"Auto-save milestone: {lead_count:,} leads scraped for {PART_ID}"
            subprocess.run(["git", "commit", "-m", commit_msg], cwd=repo_root, capture_output=True)
            
            subprocess.run(["git", "pull", "--rebase", "origin", "main"], cwd=repo_root, capture_output=True)
            push_res = subprocess.run(["git", "push", "origin", "HEAD:main"], cwd=repo_root, capture_output=True, text=True)
            
            if push_res.returncode == 0:
                logger.info(f"[+] SUCCESS: Auto-saved {lead_count:,} leads directly to GitHub repository!")
            else:
                logger.warning(f"[!] Git push notice: {push_res.stderr.strip()}")
        except Exception as git_err:
            logger.warning(f"[!] Git auto-push exception: {git_err}")

    def scrape_single_pair(self, pincode, category):
        combo_key = f"{pincode}_{category}"
        if combo_key in self.completed_combos:
            return []

        leads_for_combo = []
        local_seen = set()
        search_angles = self._generate_search_angles(pincode, category)
        meta = PINCODE_METADATA.get(pincode, {})

        for q in search_angles:
            encoded_q = urllib.parse.quote(q)
            pb_str = (
                f"!1s{encoded_q}!7i20!10b1!12m59!1m5!18b1!30b1!31m1!1b1!34e1!2m4!5m1!6e2!20e3!39b1"
                f"!6m31!32i1!49b1!63m0!66b1!85b1!114b1!149b1!206b1!209b1!212b1!215b1!216b1!222b1!223b1!232b1!234b1!235b1"
                f"!246b1!253b1!260b1!262b1!266b1!270b1!271b1!273b1!280b1!281b1!291m0!294b1!302i300!303i100!10b1!12b1!13b1"
                f"!14b1!16b1!17m1!3e1!20m4!5e2!6b1!8b1!14b1!46m1!1b0!96b1!99b1!19m4!2m3!1i360!2i120!4i8!20m57!2m2!1i0"
                f"!2i20!3m2!2i4!5b1!6m6!1m2!1i86!2i86!1m2!1i408!2i240!7m33!1m3!1e1!2b0!3e3!1m3!1e2!2b1!3e2!1m3!1e2!2b0"
                f"!3e3!1m3!1e8!2b0!3e3!1m3!1e10!2b0!3e3!1m3!1e10!2b1!3e2!1m3!1e10!2b0!3e4!1m3!1e9!2b1!3e2!2b1!9b0!15m8"
                f"!1m7!1m2!1m1!1e2!2m2!1i195!2i195!3i20"
            )
            url = f"https://www.google.com/search?tbm=map&authuser=0&hl=en&gl=in&q={encoded_q}&pb={pb_str}"

            try:
                resp = self.session.get(url, timeout=(3.0, 7.0))
                time.sleep(0.10)

                if resp.status_code == 200:
                    raw_text = resp.text
                    if raw_text.startswith(")]}'"):
                        raw_text = raw_text[raw_text.find('['):]

                    data = json.loads(raw_text)
                    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list) and len(data[0]) > 1:
                        places_raw = data[0][1]
                        if isinstance(places_raw, list):
                            for p in places_raw:
                                if not isinstance(p, list) or len(p) < 15:
                                    continue
                                d = p[14]
                                if not isinstance(d, list) or len(d) <= 11:
                                    continue

                                name = d[11] if len(d) > 11 and isinstance(d[11], str) else None
                                if not name:
                                    continue

                                place_id = d[78] if len(d) > 78 and d[78] else (d[0] if len(d) > 0 else "N/A")
                                dedup_key = place_id if place_id != "N/A" else f"{name}_{pincode}".lower()

                                if dedup_key in self.seen_keys or dedup_key in local_seen:
                                    continue
                                local_seen.add(dedup_key)
                                self.seen_keys.add(dedup_key)

                                categories_list = d[13] if len(d) > 13 and isinstance(d[13], list) else []
                                primary_category = categories_list[0] if categories_list else category
                                all_categories_str = ", ".join(categories_list) if categories_list else primary_category

                                rating = d[4][7] if len(d) > 4 and isinstance(d[4], list) and len(d[4]) > 7 else None
                                reviews_count = d[4][8] if len(d) > 4 and isinstance(d[4], list) and len(d[4]) > 8 else None

                                website = "N/A"
                                if len(d) > 7 and isinstance(d[7], list) and len(d[7]) > 0 and d[7][0]:
                                    website = str(d[7][0])

                                lat = d[9][2] if len(d) > 9 and isinstance(d[9], list) and len(d[9]) > 2 else None
                                lng = d[9][3] if len(d) > 9 and isinstance(d[9], list) and len(d[9]) > 3 else None

                                address = d[39] if len(d) > 39 and d[39] else (d[18] if len(d) > 18 and d[18] else f"{name}, {pincode}, India")
                                area = d[14] if len(d) > 14 and d[14] else str(pincode)

                                phone = self._extract_phone(d)
                                place_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}" if place_id != "N/A" else "N/A"

                                record = {
                                    "business_name": name,
                                    "search_category": category,
                                    "primary_category": primary_category,
                                    "all_categories": all_categories_str,
                                    "pincode": pincode,
                                    "circle": meta.get("circle", "N/A"),
                                    "region": meta.get("region", "N/A"),
                                    "division": meta.get("division", "N/A"),
                                    "major_offices": ", ".join(meta.get("offices", [])[:3]),
                                    "phone_number": phone,
                                    "website": website,
                                    "rating": rating,
                                    "reviews_count": reviews_count,
                                    "address": address,
                                    "area": area,
                                    "latitude": lat,
                                    "longitude": lng,
                                    "place_id": place_id,
                                    "place_url": place_url,
                                    "part_id": PART_ID,
                                    "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                }
                                leads_for_combo.append(record)
                elif resp.status_code == 429:
                    logger.warning(f"Rate limited on ({pincode}, {category}). Backing off 3s...")
                    time.sleep(3.0)
            except Exception as err:
                logger.debug(f"Notice for ({pincode}, {category}): {err}")

        if leads_for_combo:
            safe_cat = re.sub(r'[^a-zA-Z0-9_]', '_', category).strip('_').lower()
            out_json = os.path.join(self.combos_dir, f"{pincode}_{safe_cat}.json")
            out_csv = os.path.join(self.combos_dir, f"{pincode}_{safe_cat}.csv")
            try:
                with open(out_json, 'w', encoding='utf-8') as f:
                    json.dump(leads_for_combo, f, indent=2, ensure_ascii=False)
                df_c = pd.DataFrame(leads_for_combo)
                df_c.to_csv(out_csv, index=False, encoding='utf-8-sig')
            except Exception as e:
                logger.warning(f"Failed to write combo files: {e}")

        self.completed_combos.add(combo_key)
        return leads_for_combo

    def crawl_all(self):
        all_combinations = [(p, c) for p in ASSIGNED_PINCODES for c in CATEGORIES]
        remaining = [(p, c) for (p, c) in all_combinations if f"{p}_{c}" not in self.completed_combos]
        total_tasks = len(all_combinations)

        logger.info("=" * 60)
        logger.info(f"STARTING CRAWLER PART          : {PART_ID}")
        logger.info(f"Assigned PIN Codes             : {len(ASSIGNED_PINCODES):,}")
        logger.info(f"Target Categories              : {len(CATEGORIES):,}")
        logger.info(f"Total Combinations (Tasks)     : {total_tasks:,}")
        logger.info(f"Remaining Combinations         : {len(remaining):,}")
        logger.info(f"Workers / Concurrency          : {self.max_workers} Threads")
        logger.info("=" * 60)

        completed_count = total_tasks - len(remaining)
        chunk_size = 500

        for chunk_idx in range(0, len(remaining), chunk_size):
            chunk = remaining[chunk_idx:chunk_idx + chunk_size]
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_map = {executor.submit(self.scrape_single_pair, pin, cat): (pin, cat) for pin, cat in chunk}
                for future in as_completed(future_map):
                    pin, cat = future_map[future]
                    completed_count += 1
                    try:
                        records = future.result()
                        if records:
                            self.results.extend(records)
                            logger.info(f"[{completed_count}/{total_tasks}] ({pin} | {cat}) -> Extracted {len(records)} leads | Total: {len(self.results):,} leads")
                            
                            if len(self.results) - self.last_git_push_count >= LEAD_AUTO_SAVE_THRESHOLD:
                                self.last_git_push_count = len(self.results)
                                self.git_auto_push_milestone(len(self.results))
                    except Exception as e:
                        logger.error(f"Error crawling ({pin}, {cat}): {e}")

            self.save_checkpoint()
            if len(self.results) - self.last_git_push_count >= LEAD_AUTO_SAVE_THRESHOLD:
                self.last_git_push_count = len(self.results)
                self.git_auto_push_milestone(len(self.results))

        self.export_all()
        self.git_auto_push_milestone(len(self.results))
        return len(self.results)

    def export_all(self):
        if not self.results:
            logger.warning("No results to export.")
            return

        for idx, item in enumerate(self.results):
            item["s_no"] = idx + 1

        fields = [
            "s_no", "business_name", "search_category", "primary_category", "all_categories",
            "pincode", "circle", "region", "division", "major_offices",
            "phone_number", "website", "rating", "reviews_count",
            "address", "area", "latitude", "longitude", "place_id", "place_url",
            "part_id", "crawled_at"
        ]

        # 1. Master Output (CSV and JSON)
        master_csv = os.path.join(self.master_dir, f"ALL_INDIA_LEADS_{PART_ID.upper()}.csv")
        master_json = os.path.join(self.master_dir, f"ALL_INDIA_LEADS_{PART_ID.upper()}.json")
        df_master = pd.DataFrame(self.results)
        df_master.to_csv(master_csv, index=False, encoding='utf-8-sig')
        with open(master_json, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported Master: {len(self.results):,} leads to CSV and JSON")

        # 2. By Pincode Output (CSV and JSON)
        by_pin = {}
        for r in self.results:
            by_pin.setdefault(str(r.get("pincode")), []).append(r)
        for pin, pin_leads in by_pin.items():
            if not pin: continue
            df_p = pd.DataFrame(pin_leads)
            df_p.to_csv(os.path.join(self.by_pincode_dir, f"{pin}.csv"), index=False, encoding='utf-8-sig')
            with open(os.path.join(self.by_pincode_dir, f"{pin}.json"), 'w', encoding='utf-8') as f:
                json.dump(pin_leads, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported by_pincode: {len(by_pin)} pincode files (both .csv & .json)")

        # 3. By Category Output (CSV and JSON)
        by_cat = {}
        for r in self.results:
            by_cat.setdefault(str(r.get("search_category")), []).append(r)
        for cat, cat_leads in by_cat.items():
            safe_cat = re.sub(r'[^a-zA-Z0-9_]', '_', cat).strip('_').lower()
            df_c = pd.DataFrame(cat_leads)
            df_c.to_csv(os.path.join(self.by_category_dir, f"{safe_cat}.csv"), index=False, encoding='utf-8-sig')
            with open(os.path.join(self.by_category_dir, f"{safe_cat}.json"), 'w', encoding='utf-8') as f:
                json.dump(cat_leads, f, indent=2, ensure_ascii=False)
        logger.info(f"[+] Exported by_category: {len(by_cat)} category files (both .csv & .json)")

def main():
    crawler = SplitPincodeLeadCrawler(max_workers=16)
    crawler.crawl_all()

if __name__ == "__main__":
    main()
