"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 350 / 400
================================================================================
- Group: ThirdTest_pincode_group_35_parts_341_to_350
- Assigned PIN Codes: 48 (Range: 762021 to 764046)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_350.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_350.csv & .json
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

PART_ID = "part_350"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-350] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "762021",
  "762022",
  "762023",
  "762024",
  "762026",
  "762027",
  "762028",
  "762029",
  "762030",
  "762100",
  "762101",
  "762102",
  "762103",
  "762104",
  "762105",
  "762106",
  "762107",
  "762109",
  "762110",
  "762112",
  "763001",
  "763002",
  "763003",
  "763004",
  "763008",
  "764001",
  "764002",
  "764003",
  "764004",
  "764005",
  "764006",
  "764011",
  "764014",
  "764020",
  "764021",
  "764027",
  "764028",
  "764036",
  "764037",
  "764038",
  "764039",
  "764040",
  "764041",
  "764042",
  "764043",
  "764044",
  "764045",
  "764046"
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
  "762021": {
    "pincode": "762021",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Phulbani Division",
    "offices": [
      "Bamunigaon S.O (Kandhamal)",
      "Allanjuri B.O",
      "Gadapur B.O",
      "Gudikia B.O",
      "Hatimunda B.O",
      "Jhinjiriguda B.O",
      "Kasabasa B.O",
      "Kattingia B.O",
      "Kumbharigaon B.O",
      "Mahagudi B.O",
      "Mandipanka B.O",
      "Minjapanka B.O",
      "Salimagochha B.O",
      "Sangadama B.O",
      "Saramuli B.O",
      "Tajungia B.O",
      "Tamangi B.O"
    ]
  },
  "762022": {
    "pincode": "762022",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Phulbani Division",
    "offices": [
      "Kalinga S.O",
      "Balumaha B.O",
      "Duguripadi B.O",
      "Kurmingia B.O",
      "Talarimaha B.O"
    ]
  },
  "762023": {
    "pincode": "762023",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Phulbani Division",
    "offices": [
      "Jhadrajing S.O",
      "Adenigarh B.O",
      "Gundribida B.O",
      "Karanjakata B.O",
      "Laxmanpur B.O",
      "Ranipathar B.O"
    ]
  },
  "762024": {
    "pincode": "762024",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Phulbani Division",
    "offices": [
      "Biranarasinghpur S.O",
      "Bamanda B.O",
      "Bilabadi B.O",
      "Biramchandrapur B.O",
      "Devgarh B.O",
      "Karadi B.O",
      "Kelakata B.O",
      "Lunibahal B.O",
      "Mathura B.O",
      "Rambhikata B.O",
      "Sampoch B.O"
    ]
  },
  "762026": {
    "pincode": "762026",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Phulbani Division",
    "offices": [
      "Sarsara S.O",
      "Badhigaon B.O",
      "Baghiapada B.O",
      "Bandhapathar B.O",
      "Birigarh B.O",
      "Dhadalapada B.O",
      "Khuntiapada B.O",
      "Salki B.O",
      "Tikiripada B.O"
    ]
  },
  "762027": {
    "pincode": "762027",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Phulbani Division",
    "offices": [
      "Linepada S.O",
      "Bapalamendi B.O",
      "Dankuni B.O",
      "Jhagadapata B.O",
      "R Nuagaon B.O"
    ]
  },
  "762028": {
    "pincode": "762028",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Phulbani Division",
    "offices": [
      "Chakapada S.O",
      "Bellapadar B.O",
      "Brahmanpad B.O",
      "Chahali B.O",
      "Ghatiguda B.O",
      "Nediguda B.O",
      "Pasara B.O",
      "Rupagaon B.O"
    ]
  },
  "762029": {
    "pincode": "762029",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Phulbani Division",
    "offices": [
      "Gumagarh S.O",
      "Adasipada B.O",
      "Badabhuin B.O",
      "Balaskumpa B.O",
      "Boida B.O",
      "Ganjuguda B.O",
      "Keredi B.O",
      "Minia B.O",
      "Pakanagaon B.O",
      "Rasimendi B.O",
      "Titrapanga B.O"
    ]
  },
  "762030": {
    "pincode": "762030",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Phulbani Division",
    "offices": [
      "Baghiabahal S.O",
      "Barapadar B.O",
      "Dapalla B.O",
      "Gabjore B.O",
      "Khaliapalli B.O",
      "Khatakhatia B.O",
      "Lokapada B.O",
      "Rundimahal B.O",
      "Similipadar B.O"
    ]
  },
  "762100": {
    "pincode": "762100",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Phulbani Division",
    "offices": [
      "G.Udayagiri S.O",
      "Dadapada B.O",
      "Gadaguda B.O",
      "Gressingia B.O",
      "Kanbageri B.O",
      "Kathingia B.O",
      "Lingagarh B.O",
      "Mallickpodi B.O",
      "Nilungia B.O",
      "Raikala B.O",
      "Raipalli B.O",
      "Rattingia B.O",
      "Rudangia B.O",
      "Sirki B.O",
      "Sudhipada B.O",
      "Tiangia B.O",
      "Pukulingia B.O",
      "Court Street G Udayagiri S.O"
    ]
  },
  "762101": {
    "pincode": "762101",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Phulbani Division",
    "offices": [
      "Raikia S.O",
      "Bakingia B.O",
      "Bandha B.O",
      "Beredakia B.O",
      "Chanchedi B.O",
      "Dadingia B.O",
      "Deegi B.O",
      "Gedingia B.O",
      "Gudrikia B.O",
      "Gundhani B.O",
      "Kajuri B.O",
      "Lamungia B.O",
      "Mandakia B.O",
      "Mandasaru B.O",
      "Manikeswar B.O",
      "Padunbadi B.O",
      "Sikaketa B.O",
      "Sugudabadi B.O"
    ]
  },
  "762102": {
    "pincode": "762102",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Phulbani Division",
    "offices": [
      "Nuagaon S.O (Kandhamal)",
      "Balligada B.O",
      "Demisingia B.O",
      "Dharampur B.O",
      "Gunjibadi B.O",
      "Kanjamendi B.O",
      "Kudutuli B.O",
      "Lethingia B.O",
      "Lokebadi B.O",
      "Sirtiguda B.O"
    ]
  },
  "762103": {
    "pincode": "762103",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Phulbani Division",
    "offices": [
      "Balliguda S.O",
      "Badagaon B.O",
      "Bediketa B.O",
      "Boudingia B.O",
      "Budaguda B.O",
      "Budrukia B.O",
      "Burangia B.O",
      "Dandapadar B.O",
      "Demingia B.O",
      "Gumikia B.O",
      "Kachhabali B.O",
      "Kudupanga B.O",
      "Mahasingi B.O",
      "Pakari B.O",
      "Parigada B.O",
      "Rutungia B.O",
      "Sainipada B.O",
      "Samanbadi B.O",
      "Sindrigaon B.O",
      "Tillory B.O",
      "Nuasahi S.O"
    ]
  },
  "762104": {
    "pincode": "762104",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Phulbani Division",
    "offices": [
      "Daringbadiso S.O",
      "Badabanga B.O",
      "Badangia B.O",
      "Bhramarabadi B.O",
      "Danekbadi B.O",
      "Dasingbadi B.O",
      "Greenbadi B.O",
      "Kerubadi B.O",
      "Kirikuti B.O",
      "Kumbharabadi B.O",
      "Pangali B.O",
      "Parthamaha B.O",
      "Siangbali B.O",
      "Simanbadi B.O",
      "Sonepur B.O",
      "Sraniketa B.O",
      "Tekangia B.O",
      "Terabadi B.O"
    ]
  },
  "762105": {
    "pincode": "762105",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Phulbani Division",
    "offices": [
      "Kothagarh S.O",
      "Bandapipili B.O",
      "Durgapanga B.O",
      "Gugurumaha B.O",
      "Judabali B.O",
      "Kutiguda B.O",
      "Madaguda B.O",
      "Marlang B.O",
      "Ora B.O",
      "Paikadharakote B.O",
      "Salkibudaguda B.O",
      "Srirampur B.O",
      "Subarnagiri B.O"
    ]
  },
  "762106": {
    "pincode": "762106",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Phulbani Division",
    "offices": [
      "Sarangada S.O",
      "Bariguda B.O",
      "Banegaon B.O",
      "Guttingia B.O",
      "Katadi B.O",
      "Bandaguda B.O",
      "Magadangia B.O",
      "Padangi B.O",
      "Paderipada B.O",
      "Siringia B.O"
    ]
  },
  "762107": {
    "pincode": "762107",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Phulbani Division",
    "offices": [
      "Tumudibandha S.O",
      "Belghar B.O",
      "Bhandrangi B.O",
      "Bilamala B.O",
      "Gumma B.O",
      "Jalespata B.O",
      "Jhiripani B.O",
      "Lankagada B.O",
      "Mandalapadar B.O",
      "Mundigarh B.O",
      "Sirla B.O"
    ]
  },
  "762109": {
    "pincode": "762109",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Phulbani Division",
    "offices": [
      "Kurtamgarh S.O",
      "Balimusti B.O",
      "Bataguda B.O",
      "Biringia B.O",
      "Dadakangia B.O",
      "Dangesiguda B.O",
      "Mediakia B.O",
      "Parampanga B.O",
      "Pusangia B.O"
    ]
  },
  "762110": {
    "pincode": "762110",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Phulbani Division",
    "offices": [
      "Barakhama S.O",
      "Balakia B.O",
      "Khamankhole B.O",
      "Kutikia B.O",
      "Melsikia B.O",
      "Pokharibandha B.O",
      "Rabingia B.O",
      "Salaguda B.O",
      "Sarkipada B.O",
      "Sautikia B.O",
      "Sudra B.O",
      "Tikarabaju B.O"
    ]
  },
  "762112": {
    "pincode": "762112",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Phulbani Division",
    "offices": [
      "Paburia S.O",
      "Adasikupa B.O",
      "Burbinaju B.O",
      "Gardingia B.O",
      "Luhuringia B.O",
      "Petapanga B.O"
    ]
  },
  "763001": {
    "pincode": "763001",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Sunabeda 1 S.O"
    ]
  },
  "763002": {
    "pincode": "763002",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Sunabeda 2 S.O",
      "R Zone(Sunabeda) S.O",
      "Sector VII(Sunabeda) S.O"
    ]
  },
  "763003": {
    "pincode": "763003",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "D.P.Camp, Sunabeda S.O"
    ]
  },
  "763004": {
    "pincode": "763004",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Nad, Sunabeda S.O"
    ]
  },
  "763008": {
    "pincode": "763008",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Damanjodi S.O",
      "Bhitarguda B.O",
      "Litiguda B.O",
      "Mathalput B.O"
    ]
  },
  "764001": {
    "pincode": "764001",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Jeypore(K) H.O",
      "B.D.Pur S.O",
      "Jeypore(K) Civil Court S.O",
      "Jeypore(K) Koraput Road S.O",
      "Sriram Nagar S.O"
    ]
  },
  "764002": {
    "pincode": "764002",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Jeypore(K) R.S. S.O",
      "Asana B.O",
      "Bagderi B.O",
      "Banuaguda B.O",
      "Bhejahandi B.O",
      "Bhusanguda B.O",
      "D.Paunsi B.O",
      "Dhanpur B.O",
      "Kaliagam B.O",
      "Karimiti B.O",
      "Kundura B.O",
      "Lima B.O",
      "Phampuni B.O",
      "Phupugaon B.O",
      "Rondapalli B.O",
      "Taraput B.O",
      "Umuri B.O",
      "Badajiuna B.O"
    ]
  },
  "764003": {
    "pincode": "764003",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Prasadraopeta S.O",
      "Dabuguda B.O",
      "Dongerchinchi B.O",
      "Gadapadar B.O",
      "Peta B.O",
      "Tankua B.O"
    ]
  },
  "764004": {
    "pincode": "764004",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Jeypore(K) Irrigation Colony S.O"
    ]
  },
  "764005": {
    "pincode": "764005",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Jayanagar S.O (Koraput)",
      "Anta B.O",
      "Bhaminiput B.O",
      "Digapur B.O",
      "Ghumar B.O",
      "Raniput B.O"
    ]
  },
  "764006": {
    "pincode": "764006",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Bariniput S.O"
    ]
  },
  "764011": {
    "pincode": "764011",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Kolab Nagar S.O",
      "Deoghati B.O"
    ]
  },
  "764014": {
    "pincode": "764014",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Narayanpatna S.O",
      "Balipeta B.O",
      "Jogipalur B.O",
      "Khumbari B.O",
      "Podapadar B.O",
      "T.Gumandi B.O",
      "T.Padar B.O",
      "Bijaghati B.O"
    ]
  },
  "764020": {
    "pincode": "764020",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Koraput H.O",
      "Koraput Collectorate S.O"
    ]
  },
  "764021": {
    "pincode": "764021",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Omp Line S.O",
      "A.Malkangiri B.O",
      "Dumuriput B.O",
      "Kendar B.O",
      "Mahadeiput B.O",
      "Mastiput B.O",
      "Nandigam B.O",
      "P.Phulbeda B.O",
      "Suku B.O",
      "Umuri B.O",
      "Manbar B.O",
      "Koraput R.S. S.O"
    ]
  },
  "764027": {
    "pincode": "764027",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Bandhugam S.O",
      "Almonda B.O",
      "Borigi B.O",
      "Garidi B.O",
      "Jabaguda B.O",
      "Kabirbadi B.O",
      "Kaploda B.O",
      "Kumbharput B.O",
      "Nilabadi B.O",
      "Padavalda B.O",
      "Ranjiput B.O",
      "Sadabari B.O",
      "Langalabeda B.O",
      "Kanagam B.O",
      "Kumbhaganda B.O",
      "Kutrabeda B.O"
    ]
  },
  "764028": {
    "pincode": "764028",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Dasamanthpur S.O",
      "Parajabedapadar B.o",
      "Chanabeda B.O",
      "Dambaguda B.O",
      "Girliguma B.O",
      "Lulla B.O",
      "Murkur B.O",
      "Pindapadar B.O",
      "Chikambo B.O"
    ]
  },
  "764036": {
    "pincode": "764036",
    "circle": "Odisha circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "D.Padar B.O",
      "Dudhari B.O",
      "Hataguda B.O",
      "K.Guda B.O",
      "Khudi B.O",
      "Kunduli B.O",
      "P.Muthai B.O",
      "Pakjhola B.O",
      "Rajput B.O",
      "Renga B.O",
      "Upperkanti B.O",
      "Nandaka B.O",
      "Semiliguda S.O",
      "Charangul B.O"
    ]
  },
  "764037": {
    "pincode": "764037",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Nandapur S.O",
      "Umbel B.O",
      "B.Maliput B.O",
      "Bheja B.O",
      "Guntaput B.O",
      "Hakimput B.O",
      "Kadamguda B.O",
      "Kasandi B.O",
      "Khurjee B.O",
      "Kumargandhana B.O",
      "Malbelagam B.O",
      "Pithaguda B.O",
      "Simla B.O",
      "Subai B.O",
      "T.Thuba B.O"
    ]
  },
  "764038": {
    "pincode": "764038",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Padwa S.O (Koraput)",
      "Balda B.O",
      "Chatwa B.O",
      "Golur B.O",
      "Hatibari B.O",
      "Kularsingi B.O",
      "Panthulunga B.O",
      "Raisingi B.O",
      "Wandergada B.O"
    ]
  },
  "764039": {
    "pincode": "764039",
    "circle": "Odisha circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Chendika B.O",
      "Deopotangi B.O",
      "Gangarajpur B.O",
      "Kotiya B.O",
      "Maliput B.O",
      "Ondrangi B.O",
      "Peturu B.O",
      "Pukali B.O",
      "Ralegada B.O",
      "Sadam B.O",
      "Sambai B.O",
      "Sorada B.O",
      "Sunki B.O",
      "Turia B.O",
      "Nuagaon B.O",
      "Pottangi S.O",
      "Ampawalli B.O"
    ]
  },
  "764040": {
    "pincode": "764040",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Machhkund S.O",
      "Badigada B.O",
      "Godihanjar B.O",
      "Guneipada B.O"
    ]
  },
  "764041": {
    "pincode": "764041",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Jolaput S.O",
      "Attanda B.O",
      "Badel B.O",
      "Balel B.O",
      "Bilaput B.O"
    ]
  },
  "764042": {
    "pincode": "764042",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Onukadeli S.O",
      "Ondrahal B.O",
      "Upperdural B.O"
    ]
  },
  "764043": {
    "pincode": "764043",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Boipariguda S.O",
      "Baligam B.O",
      "Bodaput B.O",
      "Chikima B.O",
      "Chipakhor B.O",
      "Dandabadi B.O",
      "Dasamanthpur B.O",
      "Doraguda B.O",
      "Gondal B.O",
      "Haladikund B.O",
      "Kenduguda B.O",
      "Kollar B.O",
      "Majhiguda B.O",
      "Mohuli B.O",
      "Mosigam B.O",
      "Ramgiri B.O",
      "Tentuligumma B.O",
      "Kathapoda B.O",
      "Chandrapoda B.O"
    ]
  },
  "764044": {
    "pincode": "764044",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Mathili S.O",
      "Ambaguda B.O",
      "Bhejaguda B.O",
      "Birlaxmipur B.O",
      "Chaulmendi B.O",
      "Chedenga B.O",
      "Dalapatiguda B.O",
      "Dungiaput B.O",
      "Gongola B.O",
      "Govindpalli B.O",
      "Kartanpalli B.O",
      "Kiyanga B.O",
      "Kottapali B.O",
      "Kutunipalli B.O",
      "Mekka B.O",
      "Mendukuli B.O",
      "Mohupadar B.O",
      "Naikaguda B.O",
      "Pandripani B.O",
      "Pongam B.O",
      "Salimi B.O",
      "Temurupali B.O",
      "Udilibeda B.O",
      "Bhandari Pangam B.O",
      "Pedawada B.O",
      "SERPALLI B.O",
      "Madakapadar B.O",
      "Markapalli B.O",
      "Kamarpalli B.O",
      "Parajabadapada B.O"
    ]
  },
  "764045": {
    "pincode": "764045",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Malkangiri S.O",
      "Undrukonda B.O",
      "Tigal B.O",
      "Badigeta B.O",
      "Chintalwada B.O",
      "Chalanguda B.O",
      "Goudaguda B.O",
      "Jharapalli B.O",
      "Korukonda B.O",
      "Mariwada B.O",
      "Padmagiri B.O",
      "Panchabati B.O",
      "Pedakonda B.O",
      "Tamasa B.O",
      "Tandiki B.O",
      "Tumusapalli B.O",
      "Udupa B.O",
      "Dudameta B.O"
    ]
  },
  "764046": {
    "pincode": "764046",
    "circle": "Odisha Circle",
    "region": "Berhampur Region",
    "division": "Koraput Division",
    "offices": [
      "Khairaput S.O",
      "Kadamguda B.O",
      "Podaghat B.O",
      "Kudumuluguma B.O",
      "Somnathpur B.O",
      "Mudulipada B.O",
      "Mundiguda B.O",
      "Rasbeda B.O",
      "Parkonmala B.O"
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
