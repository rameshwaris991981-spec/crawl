"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 341 / 400
================================================================================
- Group: ThirdTest_pincode_group_35_parts_341_to_350
- Assigned PIN Codes: 48 (Range: 754209 to 755012)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_341.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_341.csv & .json
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

PART_ID = "part_341"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-341] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "754209",
  "754210",
  "754211",
  "754212",
  "754213",
  "754214",
  "754215",
  "754216",
  "754217",
  "754218",
  "754219",
  "754220",
  "754221",
  "754222",
  "754223",
  "754224",
  "754225",
  "754227",
  "754228",
  "754231",
  "754239",
  "754240",
  "754244",
  "754245",
  "754246",
  "754248",
  "754250",
  "754253",
  "754289",
  "754290",
  "754292",
  "754293",
  "754294",
  "754295",
  "754296",
  "754297",
  "754298",
  "755001",
  "755003",
  "755004",
  "755005",
  "755006",
  "755007",
  "755008",
  "755009",
  "755010",
  "755011",
  "755012"
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
  "754209": {
    "pincode": "754209",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack City Division",
    "offices": [
      "ASURESWAR S.O",
      "AGYANPUR B.O",
      "BAGHUNI B.O",
      "GHANTALO B.O",
      "HARIRAJPUR B.O",
      "KULASUKARAPADA B.O",
      "KUSUNPUR B.O",
      "KATIKATA B.O",
      "TAROTASASAN B.O",
      "SUKARPADA B.O"
    ]
  },
  "754210": {
    "pincode": "754210",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Danpur S.O (Kendrapara)",
      "Chanchol B.O",
      "Gahaga Narasinghpur B.O",
      "Harianka B.O",
      "Indalo B.O",
      "Janra Barimul B.O",
      "Mahal B.O",
      "Pakhyat B.O",
      "Sorisia B.O"
    ]
  },
  "754211": {
    "pincode": "754211",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Kendrapara H.O",
      "Kendrapara College S.O",
      "Keshpur Bazar S.O",
      "Tinimuhani S.O"
    ]
  },
  "754212": {
    "pincode": "754212",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Sribaldevjew S.O",
      "Alailo B.O",
      "Anduli B.O",
      "Angulai B.O",
      "Ayaba B.O",
      "Bagada B.O",
      "Balipal B.O",
      "Deulipara B.O",
      "Kalapara B.O",
      "Mantiri B.O",
      "Nembra B.O",
      "Ostapur B.O",
      "Purusottampur B.O",
      "Rajgarh B.O",
      "Thauri B.O"
    ]
  },
  "754213": {
    "pincode": "754213",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Marsaghai S.O",
      "Ashram Balikuda B.O",
      "Badakula B.O",
      "Balana B.O",
      "Dumuka B.O",
      "Garjanga B.O",
      "Jadupur B.O",
      "Naupada B.O",
      "Pikarali B.O",
      "Sasanipada B.O",
      "Silipur B.O",
      "Talasanga B.O"
    ]
  },
  "754214": {
    "pincode": "754214",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Indupur S.O",
      "Alapua B.O",
      "Aliabad B.O",
      "Amathpur B.O",
      "Baragaon Mahakalpara B.O",
      "Chhoti B.O",
      "Ghagra B.O",
      "Gommu B.O",
      "Kampagarh B.O",
      "Kolangiri B.O",
      "Palli Raghunathpur B.O",
      "Ratnagiri B.O",
      "Kutranga B.O",
      "Nikirai B.O",
      "Rout Sahi B.O"
    ]
  },
  "754215": {
    "pincode": "754215",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Pattamundai S.O",
      "Ainipara B.O",
      "Amber B.O",
      "Baliapatna B.O",
      "Bankeswar B.O",
      "Beltal B.O",
      "Bilikana (Gopalpur) B.O",
      "Kasananta B.O",
      "Malapatna B.O",
      "Manikapatna B.O",
      "Narsinghpur Dihasahi B.O",
      "Patrapur B.O",
      "Sahira B.O",
      "Sandhapalli B.O",
      "Sekhpur B.O",
      "Singiri B.O",
      "Badamulabasanta B.O",
      "Khadianta B.O",
      "Pattamundai College S.O"
    ]
  },
  "754216": {
    "pincode": "754216",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Jaria S.O (Kendrapara)",
      "Gunupur B.O",
      "Jigiran B.O",
      "Sana Jaria B.O"
    ]
  },
  "754217": {
    "pincode": "754217",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Alba S.O",
      "Badapalli B.O",
      "Balabhadrapur B.O",
      "Chandan Nagar B.O",
      "Damarpur B.O",
      "Dosia B.O",
      "Kakharuni B.O",
      "Oudapada B.O",
      "Sansarphal B.O",
      "Sasan B.O",
      "Srirampalpatna B.O",
      "Terohi B.O"
    ]
  },
  "754218": {
    "pincode": "754218",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Batipada S.O",
      "Amrutamanohi B.O",
      "Baluria B.O",
      "Nuapada B.O",
      "Sanamanga B.O",
      "Sansidha Mangarajpur B.O"
    ]
  },
  "754219": {
    "pincode": "754219",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Aul S.O",
      "Balakati B.O",
      "Dasipur B.O",
      "Mendhapur B.O",
      "Nihala B.O",
      "Podamarai Alakana B.O",
      "Sitaleswar B.O"
    ]
  },
  "754220": {
    "pincode": "754220",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Rajkanika S.O",
      "Achyutpur B.O",
      "Ayatan B.O",
      "Baradia B.O",
      "Barunadiha B.O",
      "Bharigada B.O",
      "Dalikainda B.O",
      "Giria B.O",
      "Jagulaipara B.O",
      "Jaynagar B.O",
      "Katna B.O",
      "Koilipur B.O",
      "Pimpudi B.O",
      "Siko B.O",
      "Sikudi B.O",
      "Taras B.O"
    ]
  },
  "754221": {
    "pincode": "754221",
    "circle": "Odisha circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack City Division",
    "offices": [
      "BADAGOTHA B.O",
      "BHIMDASPUR B.O",
      "KOOD B.O",
      "KUSUMBI B.O",
      "MIRZAPUR B.O",
      "PIKOL B.O",
      "PALLISAHI B.O",
      "SUKLESWAR B.O",
      "SUNGRA S.O"
    ]
  },
  "754222": {
    "pincode": "754222",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Chaudakulat S.O",
      "Basupur B.O",
      "Kadaliban B.O",
      "Kora B.O",
      "Gogua B.O",
      "Palasingha B.O"
    ]
  },
  "754223": {
    "pincode": "754223",
    "circle": "Odisha circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Balasore Division",
    "offices": [
      "Purusottampur B.O",
      "Karilopatna S.O",
      "Aitipur B.O",
      "Gopei B.O",
      "Nadia Barei B.O",
      "Raichand B.O",
      "Aripada B.O",
      "Chakroda B.O",
      "Bharatpur B.O",
      "Kantia B.O"
    ]
  },
  "754224": {
    "pincode": "754224",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Mahakalpara S.O",
      "Balia B.O",
      "Baradanga B.O",
      "Barakanda B.O",
      "Bijayanagar B.O",
      "Gojabandha B.O",
      "Jamboo B.O",
      "Jasuapalli B.O",
      "Kansar Badadandua B.O",
      "Karanja B.O",
      "Kharinasi B.O",
      "Kiarbanka B.O",
      "Kochila B.O",
      "Maliancha B.O",
      "Nantar B.O",
      "Ramnagar Refugee Colony B.O",
      "Suniti B.O",
      "Teragaon B.O"
    ]
  },
  "754225": {
    "pincode": "754225",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Rajnagar S.O (Kendrapara)",
      "Badanaukana B.O",
      "Bhitaragarh B.O",
      "Dera B.O",
      "Gopalpur Rajnagar B.O",
      "Gupti B.O",
      "Junapangara B.O",
      "Kandira B.O",
      "Kurunti B.O",
      "Mahulia B.O",
      "Satabhaya B.O"
    ]
  },
  "754227": {
    "pincode": "754227",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Olavar S.O",
      "Bahmanda B.O",
      "Baruna B.O",
      "Chhadesh B.O",
      "Manapur B.O",
      "Nuagaon B.O",
      "Pegarpara B.O"
    ]
  },
  "754228": {
    "pincode": "754228",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Patakura S.O (Kendrapara)",
      "Akhua Dakhini B.O",
      "Pundalo B.O"
    ]
  },
  "754231": {
    "pincode": "754231",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Namouza S.O",
      "Areikana B.O",
      "Atal B.O",
      "Mahu B.O"
    ]
  },
  "754239": {
    "pincode": "754239",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Bhuinpur S.O",
      "Kalaspur B.O",
      "Padanipal B.O"
    ]
  },
  "754240": {
    "pincode": "754240",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Dandisahi S.O",
      "Andara B.O",
      "Penthapal B.O",
      "Taradapal B.O"
    ]
  },
  "754244": {
    "pincode": "754244",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Jampara S.O",
      "Bachharai B.O",
      "Bandhakata B.O",
      "Mahendinagar B.O",
      "Pentha B.O",
      "Raghunathpur K Patna B.O"
    ]
  },
  "754245": {
    "pincode": "754245",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Barpada S.O",
      "Babar B.O",
      "Badihi B.O",
      "Chakada Gogua B.O"
    ]
  },
  "754246": {
    "pincode": "754246",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Madanpur S.O (Kendrapara)",
      "Bandhapatna B.O",
      "Bhatapada [r] B.O",
      "Jagannathpur Sasan B.O",
      "Keradagada B.O",
      "Koilipur B.O"
    ]
  },
  "754248": {
    "pincode": "754248",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Iswarpur S.O",
      "Dangamal B.O",
      "Ghadiamal B.O",
      "Krishnanagar B.O",
      "Manjulapalli B.O",
      "Nalitapatia B.O",
      "Rangani B.O",
      "Talachua B.O",
      "Yunus Nagar B.O"
    ]
  },
  "754250": {
    "pincode": "754250",
    "circle": "Odisha circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Badagaon B.O",
      "Thakurpatna S.O",
      "Alati Bharatpur B.O",
      "Barua B.O",
      "Belarpur B.O",
      "Fakirabad B.O",
      "Gopaljewpatna B.O",
      "Kusiapal B.O",
      "Panasudha B.O",
      "Pandiri B.O",
      "Jamdhar B.O",
      "Tilotamadeipur B.O",
      "Gulnagar B.O",
      "Bira Nilakanthapur B.O",
      "Kapaleswar B.O"
    ]
  },
  "754253": {
    "pincode": "754253",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Bedari S.O",
      "Mandia B.O",
      "Padmapur B.O"
    ]
  },
  "754289": {
    "pincode": "754289",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Derabish S.O",
      "Balia Bibhutipara B.O",
      "Chatrachakada B.O",
      "Jagulaipara B.O",
      "Laxminarayanpur B.O",
      "Nuahat B.O"
    ]
  },
  "754290": {
    "pincode": "754290",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack South Division",
    "offices": [
      "Mouda S.O",
      "Bhera B.O",
      "Chasakhanda B.O",
      "Tilda B.O"
    ]
  },
  "754292": {
    "pincode": "754292",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Charinangal S.O",
      "Benipur B.O",
      "Chandia B.O",
      "Kalashree Gopalpur B.O",
      "Raipur B.O",
      "Saudia B.O"
    ]
  },
  "754293": {
    "pincode": "754293",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack City Division",
    "offices": [
      "Lemalo S.O",
      "Bhagabanpur B.O",
      "Charirakaba B.O",
      "Orikanta B.O"
    ]
  },
  "754294": {
    "pincode": "754294",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack South Division",
    "offices": [
      "Chatra S.O (Jagatsinghapur)",
      "Alipingal B.O",
      "Taradapada B.O",
      "Palli B.O",
      "Odisso B.O",
      "Sitahlo B.O",
      "Palasol B.O",
      "Punanga B.O",
      "Nati B.O",
      "Mohiuddinpur B.O",
      "Sagadailo BO"
    ]
  },
  "754295": {
    "pincode": "754295",
    "circle": "Odisha circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack South Division",
    "offices": [
      "Ichhapur B.O",
      "Samsarpur B.O",
      "Athagarh Bazar S.O",
      "Badabhuin B.O",
      "Kandarai B.O",
      "Khuntakata B.O",
      "Kulailo B.O",
      "Megha B.O",
      "Radhakishorepur B.O",
      "Bentapada B.O",
      "Chakada B.O",
      "Dhaipur B.O",
      "Dorada B.O",
      "Oranda B.O",
      "Subarnapur B.O",
      "Bali B.O",
      "Sahangagopalpur B.O",
      "Arakhapatna B.O",
      "Mahakalbasta B.O",
      "Gholapur B.O",
      "Bisnupur B.O",
      "Jenapada B.O",
      "Kadalibadi B.O",
      "Karikol B.O",
      "Tarading B.O",
      "Balarampur B.O",
      "Kandarpur B.O"
    ]
  },
  "754296": {
    "pincode": "754296",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "SUNGUDA S.O",
      "Arakhpur B.O",
      "Bidyadharpur B.O",
      "Koudikol B.O",
      "Sakuntalapur B.O",
      "Barchana B.O"
    ]
  },
  "754297": {
    "pincode": "754297",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack South Division",
    "offices": [
      "Khuntuni S.O"
    ]
  },
  "754298": {
    "pincode": "754298",
    "circle": "Odisha Circle",
    "region": "N/A",
    "division": "Cuttack South Division",
    "offices": [
      "KALARABANKA S.O"
    ]
  },
  "755001": {
    "pincode": "755001",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Jajpur H.O",
      "Ankula S.O",
      "Jajpur Bazar S.O",
      "Jajpur College S.O"
    ]
  },
  "755003": {
    "pincode": "755003",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Bari Cuttack S.O",
      "Balibili B.O",
      "Bari Kalamatia B.O",
      "Dharpur B.O",
      "Mandari B.O",
      "Meduakul B.O",
      "Nathapur B.O",
      "Rajapur B.O",
      "Ramachandrapur B.O",
      "Sahupara B.O"
    ]
  },
  "755004": {
    "pincode": "755004",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Binjharpur S.O",
      "Angalo B.O",
      "Angalo Madhusudanpur B.O",
      "Barapada B.O",
      "Kantipur B.O",
      "Kapila B.O",
      "Mohanpur B.O",
      "Ratalang B.O",
      "Talabandha B.O"
    ]
  },
  "755005": {
    "pincode": "755005",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Brahmabarada S.O",
      "Abhimanyu Balia B.O",
      "Arabal B.O",
      "Bandhadihi B.O",
      "Deoda B.O",
      "Golkunda B.O",
      "Haripur B.O",
      "Isanpur B.O",
      "Janakanalkul B.O",
      "Janapada B.O",
      "Jhalpada B.O",
      "Kakudikuda B.O",
      "Rudrapur B.O"
    ]
  },
  "755006": {
    "pincode": "755006",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Dasarathpur S.O",
      "Dehurianandapur B.O",
      "Jayantara B.O",
      "Kantapari B.O",
      "Khosalpur B.O",
      "Mamadulla B.O",
      "Merdakatia Chhak B.O",
      "Nandipur B.O",
      "Pubasahi B.O",
      "Taranga Sagarpur B.O"
    ]
  },
  "755007": {
    "pincode": "755007",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Debidwar S.O",
      "Aranga Purusottampur B.O",
      "Baidyarajpur B.O",
      "Bankamuhani B.O",
      "Jahanpur B.O",
      "Khairabad B.O",
      "Kodandpur B.O",
      "Lalbag B.O",
      "Nakhei B.O",
      "Nathsahi B.O",
      "Simulia B.O",
      "Solempur B.O"
    ]
  },
  "755008": {
    "pincode": "755008",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Dharmasala S.O",
      "Areikana B.O",
      "Banamalipur B.O",
      "Barabati B.O",
      "Kadampal B.O",
      "Kaima B.O",
      "Kalan B.O",
      "Kotapur B.O",
      "Kundapatna B.O",
      "Mirzapur B.O",
      "Nahangapatna B.O",
      "Oddisso Andeigoda B.O",
      "Prathamakhandi B.O",
      "Singhapur B.O"
    ]
  },
  "755009": {
    "pincode": "755009",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Kabirpur S.O",
      "Badabanta B.O",
      "Bahdalpur B.O",
      "Haripur Hat B.O",
      "Kutchery Gaon B.O",
      "Mangarajpur B.O",
      "Mugupal B.O",
      "Rajendrapur B.O",
      "Rakabi Bazar B.O",
      "Samantarapur B.O"
    ]
  },
  "755010": {
    "pincode": "755010",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Kaipara S.O",
      "Anikana B.O",
      "Sherpur B.O"
    ]
  },
  "755011": {
    "pincode": "755011",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Mangalpur S.O (Jajapur)",
      "Dhanipur B.O",
      "Dubakana B.O",
      "Iswarpur B.O",
      "Kamardihi B.O",
      "Kanikapara B.O",
      "Kayan B.O",
      "Khannagar B.O",
      "Palatpur B.O",
      "Paripada Nizampur B.O",
      "Sanakuanlo B.O"
    ]
  },
  "755012": {
    "pincode": "755012",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Cuttack North Division",
    "offices": [
      "Mashra S.O",
      "Bachhalo B.O",
      "Balamukulihat B.O",
      "Banshipur B.O",
      "Fatehpur B.O",
      "Kalyanpur B.O",
      "Ranpur B.O",
      "Tina B.O"
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
