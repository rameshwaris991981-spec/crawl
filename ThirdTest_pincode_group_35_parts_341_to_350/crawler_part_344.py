"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 344 / 400
================================================================================
- Group: ThirdTest_pincode_group_35_parts_341_to_350
- Assigned PIN Codes: 48 (Range: 756133 to 757042)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_344.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_344.csv & .json
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

PART_ID = "part_344"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-344] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "756133",
  "756134",
  "756135",
  "756137",
  "756138",
  "756139",
  "756144",
  "756162",
  "756163",
  "756164",
  "756165",
  "756166",
  "756167",
  "756168",
  "756171",
  "756181",
  "756182",
  "757001",
  "757002",
  "757003",
  "757014",
  "757016",
  "757017",
  "757018",
  "757019",
  "757020",
  "757021",
  "757022",
  "757023",
  "757024",
  "757025",
  "757026",
  "757027",
  "757028",
  "757029",
  "757030",
  "757031",
  "757032",
  "757033",
  "757034",
  "757035",
  "757036",
  "757037",
  "757038",
  "757039",
  "757040",
  "757041",
  "757042"
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
  "756133": {
    "pincode": "756133",
    "circle": "Odisha circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Bhadrak Division",
    "offices": [
      "Sukleswar B.O",
      "Chandbali S.O",
      "Tentulidihi B.O",
      "Ugratara B.O",
      "Baligaon B.O",
      "Bentalpur B.O",
      "Bijayanagar B.O",
      "Bouljoda B.O",
      "Goladia B.O",
      "Gopalpur B.O",
      "Orasahi B.O",
      "Panchpada B.O",
      "Chandbali Bazar S.O"
    ]
  },
  "756134": {
    "pincode": "756134",
    "circle": "Odisha circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Bhadrak Division",
    "offices": [
      "Antara B.O",
      "Ada S.O",
      "Barada B.O",
      "Chandarpada B.O",
      "Garasang B.O",
      "Jalanga Gandibed B.O",
      "Mirjapur B.O"
    ]
  },
  "756135": {
    "pincode": "756135",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Bhadrak Division",
    "offices": [
      "Randiahat S.O",
      "Badabarchikayan B.O",
      "Bodakpatna B.O",
      "Kadabaranga B.O",
      "Odanga B.O",
      "Olanga B.O",
      "Saramara B.O"
    ]
  },
  "756137": {
    "pincode": "756137",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Bhadrak Division",
    "offices": [
      "Asurali S.O",
      "Fatepur B.O",
      "Gadiali B.O",
      "Govindapur B.O",
      "Kasimpur B.O",
      "Korua B.O"
    ]
  },
  "756138": {
    "pincode": "756138",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Bhadrak Division",
    "offices": [
      "Aradi S.O",
      "Batula B.O",
      "Bhuinpur B.O",
      "Nandapur B.O",
      "Olaga B.O",
      "Sathibankuda B.O"
    ]
  },
  "756139": {
    "pincode": "756139",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Bhadrak Division",
    "offices": [
      "Arsa S.O",
      "Bidyadharpur B.O",
      "Talapada B.O"
    ]
  },
  "756144": {
    "pincode": "756144",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Bhadrak Division",
    "offices": [
      "Basantia S.O (Bhadrak)",
      "Apanda B.O",
      "Silandi B.O"
    ]
  },
  "756162": {
    "pincode": "756162",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Bhadrak Division",
    "offices": [
      "Eram S.O",
      "Nuagaon B.O",
      "Suan B.O"
    ]
  },
  "756163": {
    "pincode": "756163",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Bhadrak Division",
    "offices": [
      "Guamal S.O",
      "Bhuianwash B.O",
      "Bilana B.O",
      "Bodak B.O",
      "Galagandapur B.O",
      "Jaipur B.O",
      "Kubera B.O"
    ]
  },
  "756164": {
    "pincode": "756164",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Bhadrak Division",
    "offices": [
      "Naikanidihi S.O",
      "Baincha B.O",
      "Balimunda B.O",
      "Bansada Kuamara B.O",
      "Bedeipur B.O",
      "Govindpur B.O",
      "Karanjamal B.O",
      "Karanpalli B.O",
      "Sanahavelisahi B.O"
    ]
  },
  "756165": {
    "pincode": "756165",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Bhadrak Division",
    "offices": [
      "Brahmanigaon S.O",
      "Bachhipur B.O",
      "Kiapada B.O"
    ]
  },
  "756166": {
    "pincode": "756166",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Bhadrak Division",
    "offices": [
      "Balikhanda S.O",
      "Kalaspur B.O",
      "Kanchpada B.O",
      "Parbatipur B.O",
      "Ramakrishnapur B.O",
      "Srimantapur B.O"
    ]
  },
  "756167": {
    "pincode": "756167",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Bhadrak Division",
    "offices": [
      "Paliabindha S.O",
      "Barunai B.O"
    ]
  },
  "756168": {
    "pincode": "756168",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Bhadrak Division",
    "offices": [
      "Betada S.O",
      "Aruha B.O",
      "Barandua B.O",
      "Chalunigaon B.O",
      "Kanheibindha B.O",
      "Khirkona B.O",
      "Narasinghpur B.O"
    ]
  },
  "756171": {
    "pincode": "756171",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Bhadrak Division",
    "offices": [
      "Dhamara S.O",
      "Dosinga B.O",
      "Kaithkhola B.O",
      "Kishore Prasad B.O",
      "Narasinghpurhat B.O",
      "Saraswati B.O"
    ]
  },
  "756181": {
    "pincode": "756181",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Bhadrak Division",
    "offices": [
      "Madhabnagar S.O",
      "Bhadrak Bypass B.O",
      "Garadapur B.O",
      "Gelpur B.O",
      "Geltua B.O",
      "Jalanga B.O",
      "Mouda B.O",
      "Tihiri B.O",
      "Prachinagar B.O"
    ]
  },
  "756182": {
    "pincode": "756182",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Bhadrak Division",
    "offices": [
      "Dungura S.O",
      "Aliha B.O",
      "Arakhpur B.O",
      "Baghua B.O",
      "Damodarpur B.O"
    ]
  },
  "757001": {
    "pincode": "757001",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Baripada H.O",
      "Ambikasahi S.O",
      "Baripada Bazar S.O",
      "Baripada Court S.O",
      "Baripada Hospital S.O",
      "Baripada Municipal Market S.O",
      "Baripada R.S. S.O",
      "Deulasahi S.O",
      "Palabani Chhak S.O",
      "Tulasichoura S.O"
    ]
  },
  "757002": {
    "pincode": "757002",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Bhanjpur S.O",
      "Belgadia S.O",
      "Bijay Ramchandrapur S.O"
    ]
  },
  "757003": {
    "pincode": "757003",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Takatpur S.O",
      "Dakoipodadiha B.O",
      "Damodar Pur B.O",
      "Goudadiha B.O",
      "Raikad Jharan B.O",
      "Baghra Road B.O",
      "Sripad Ganja B.O"
    ]
  },
  "757014": {
    "pincode": "757014",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Chandua Ashram S.O",
      "Baghuasole B.O",
      "Bhugudakata B.O",
      "Dardara B.O",
      "Gayalkata B.O",
      "Kakharusole B.O",
      "Keutunimari B.O",
      "Kuabuda B.O",
      "Kusumasole B.O",
      "Marangtandi B.O",
      "Saragchhida B.O",
      "Tadki B.O"
    ]
  },
  "757016": {
    "pincode": "757016",
    "circle": "Odisha circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Bachhuripal B.O",
      "Debsole B.O",
      "Gobra B.O",
      "Jharia B.O",
      "Kamardiha B.O",
      "M. Kanpur B.O",
      "Patagadia B.O",
      "Rasgovindpur S.O"
    ]
  },
  "757017": {
    "pincode": "757017",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Mantri S.O",
      "Bijipur B.O",
      "Chandanpur B.O",
      "Deopara B.O",
      "Kuradiha B.O",
      "Mirigidari B.O",
      "Nakhra B.O",
      "Nankua B.O",
      "Narangaon B.O",
      "Pruthunathpur B.O",
      "Salgaon B.O"
    ]
  },
  "757018": {
    "pincode": "757018",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Chitrada S.O",
      "Anua B.O",
      "Bhairangisole B.O",
      "Bhaliadiha B.O",
      "Bholaghati B.O",
      "Chandabilla B.O",
      "Chhatna B.O",
      "Dhansole B.O",
      "Gadigaon B.O",
      "Gholmuhan B.O",
      "Idar B.O",
      "Kapoi B.O",
      "Maigaon B.O",
      "Makunda B.O",
      "Musagadia B.O",
      "Nichuapada B.O",
      "Paramananda B.O",
      "Patpur B.O",
      "Sunahaja B.O"
    ]
  },
  "757019": {
    "pincode": "757019",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Khunta S.O",
      "Atanati B.O",
      "Badkaranjia B.O",
      "Badmamundia B.O",
      "Baldiha Khunta B.O",
      "Banakati B.O",
      "Bhandagaon B.O",
      "Hirapur B.O",
      "Karkachia B.O",
      "Silaghati B.O",
      "Tikarpada B.O"
    ]
  },
  "757020": {
    "pincode": "757020",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Moroda S.O",
      "Badampur B.O",
      "Baincha Nuagaon B.O",
      "Baladia B.O",
      "Barkand B.O",
      "Bhatchhatar B.O",
      "Chakulia B.O",
      "Ghodabandha  Khuntapal B.O",
      "Jhinkiria B.O",
      "Khuntapal B.O",
      "Manida B.O",
      "N. Chhotraipur B.O",
      "Rakhinisahi B.O",
      "Rukuni B.O"
    ]
  },
  "757021": {
    "pincode": "757021",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Deuli S.O",
      "Anlakuda B.O",
      "Chaksuliapada B.O",
      "Chuhat B.O",
      "Dhemnasole B.O",
      "Kanimahuli B.O",
      "Nuagaon talakchuin B.O",
      "Sarasposi B.O"
    ]
  },
  "757022": {
    "pincode": "757022",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Suliapada S.O",
      "Baghra B.O",
      "Bari B.O",
      "Darudihi B.O",
      "Dhatika B.O",
      "Mahabila B.O",
      "Pokharia B.O",
      "Saluadahar B.O",
      "Saradiha B.O",
      "Singda B.O",
      "Ufalgadia B.O"
    ]
  },
  "757023": {
    "pincode": "757023",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Garia S.O (Mayurbhanj)",
      "Badfera B.O",
      "Badmundhabani B.O",
      "Dhenkinenjia B.O",
      "Futukisole B.O",
      "Kandana B.O",
      "Keshrasole B.O",
      "Khalapada B.O",
      "Kui B.O",
      "Kujidihi B.O",
      "Lunia B.O",
      "Nuhamalia B.O",
      "Sanmundhabani B.O"
    ]
  },
  "757024": {
    "pincode": "757024",
    "circle": "Odisha circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Bajratundi B.O",
      "Baunsbilla B.O",
      "Belam B.O",
      "Gudialbandh B.O",
      "Jadunathpur B.O",
      "Kadualbandh B.O",
      "Kerko Baiganbaria B.O",
      "Paikabasa B.O",
      "Talanda B.O",
      "Tangasole B.O",
      "Uthani Nuagaon B.O",
      "Sankerko S.O"
    ]
  },
  "757025": {
    "pincode": "757025",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Betnoti S.O",
      "Agria B.O",
      "Aguad B.O",
      "Bartana B.O",
      "Dalki B.O",
      "Durgapur B.O",
      "Gobapal B.O",
      "Kanchisole B.O",
      "Kochilakhunta B.O",
      "Kuliana Barsahi B.O",
      "Nuagaon B.O",
      "Paunsia B.O",
      "Rangapani B.O",
      "Sakua B.O",
      "Sialighati B.O"
    ]
  },
  "757026": {
    "pincode": "757026",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Barsahi S.O",
      "B. Chhelia B.O",
      "Chadada B.O",
      "Chhelia B.O",
      "Deulia B.O",
      "Gendagadia B.O",
      "Kendudiha B.O",
      "Khuntapal B.O",
      "Patarapada B.O",
      "Ranibandh B.O",
      "Singtia B.O",
      "Suhagpura B.O",
      "Talpada B.O"
    ]
  },
  "757027": {
    "pincode": "757027",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Kuamara S.O",
      "Sikidia B.O",
      "Talakadada B.O"
    ]
  },
  "757028": {
    "pincode": "757028",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Baisinga S.O",
      "Ambagadia B.O",
      "Badkhiripada B.O",
      "Bidukudia B.O",
      "Kadalia B.O",
      "Khuntapur B.O",
      "Mahulpatna B.O",
      "Nachhipuria B.O",
      "Patalipura B.O",
      "Purunia B.O",
      "Raghupur B.O",
      "Singlamundali B.O"
    ]
  },
  "757029": {
    "pincode": "757029",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Krushnachandrapur S.O",
      "Asanbani B.O",
      "Badjod B.O",
      "Bhalia B.O",
      "Budhikhamari B.O",
      "Dantiamuhan B.O",
      "Dhanpur B.O",
      "Hatikote B.O",
      "Jamdapal B.O",
      "Jamsole B.O",
      "Karamsole B.O",
      "Sureidihi B.O",
      "Tasarda B.O"
    ]
  },
  "757030": {
    "pincode": "757030",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Kuliana S.O",
      "Ambapunja B.O",
      "Digar Astia B.O",
      "Gendapokhari B.O",
      "Ghodapal B.O",
      "Goudruma B.O",
      "Jagannathkhunta B.O",
      "Kalabadia B.O",
      "Kathsirsi B.O",
      "Koilisuta B.O",
      "Kudiapal B.O",
      "Nabedhi B.O",
      "Nuagaon B.O",
      "Parulia B.O",
      "Pathuri B.O",
      "Patihinja B.O",
      "Sansarasposi B.O"
    ]
  },
  "757031": {
    "pincode": "757031",
    "circle": "Odisha circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Bagjhumpa B.O",
      "Baunsda B.O",
      "Ekdali B.O",
      "Gandipani B.O",
      "Pandra Rolasahi B.O",
      "Sankachimbila B.O",
      "Shirsa S.O"
    ]
  },
  "757032": {
    "pincode": "757032",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Bangriposi S.O",
      "Asana B.O",
      "Bankati B.O",
      "Bedhakudar B.O",
      "Danadar B.O",
      "Dhobanisole B.O",
      "Dighi B.O",
      "Ghatkuanri B.O",
      "Golamundhakata B.O",
      "Gosanipal B.O",
      "Katra Chandan Pur B.O",
      "Manbhanj B.O",
      "Nafree B.O",
      "Nischinta B.O",
      "Pandhada B.O",
      "Patharkham B.O",
      "Puruna Pani B.O"
    ]
  },
  "757033": {
    "pincode": "757033",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Bisoi S.O",
      "Asana B.O",
      "Badnalua B.O",
      "Baneikala B.O",
      "Bautibeda B.O",
      "Kaduani B.O",
      "Khadambeda B.O",
      "Kundulia B.O",
      "Majhigaon B.O",
      "Manada B.O",
      "N.B.Pokharia B.O",
      "Nuagaon kurkutia B.O",
      "Sanjambilla B.O",
      "Sanpurunapani B.O",
      "Sargoda Jashipur B.O",
      "Sunajodia B.O"
    ]
  },
  "757034": {
    "pincode": "757034",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Jashipur S.O",
      "Astakuanr B.O",
      "Barehipani B.O",
      "Baunsnali B.O",
      "Chakidi B.O",
      "Chheligudhudi B.O",
      "Dhalabani B.O",
      "Durdura B.O",
      "Gudugudia B.O",
      "Jamukeswar B.O",
      "Moudi B.O",
      "Podagarh B.O",
      "ALKUDAR"
    ]
  },
  "757035": {
    "pincode": "757035",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Raruan S.O",
      "Angarpada B.O",
      "Baidyanath B.O",
      "Brahmaniposi B.O",
      "Budamara B.O",
      "Budhigambharia B.O",
      "Denua B.O",
      "Ghagarbeda B.O",
      "Hindala B.O",
      "Jamuti B.O",
      "Khuntapada B.O",
      "Naksara B.O",
      "Nuagaon B.O",
      "Raikala B.O",
      "Righa B.O"
    ]
  },
  "757036": {
    "pincode": "757036",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Tato S.O",
      "Badgaon B.O",
      "Bakartala B.O",
      "Bala B.O",
      "Barakamuda B.O",
      "Baria B.O",
      "Batpalasa B.O",
      "Chanchbani B.O",
      "Dari B.O",
      "Dudhiani B.O",
      "Hatibari B.O",
      "Moto B.O",
      "Pantho B.O",
      "Rugudi B.O",
      "Salarapada B.O",
      "Tongabilla B.O"
    ]
  },
  "757037": {
    "pincode": "757037",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Karanjia S.O (Mayurbhanj)",
      "Bhadubeda B.O",
      "Bhanra B.O",
      "Chitraposi B.O",
      "Ghagarda B.O",
      "Ghosada B.O",
      "Indupur B.O",
      "Kendujiani B.O",
      "Kendumundi B.O",
      "Kerkera B.O",
      "Kuliposi B.O",
      "Kushpada B.O",
      "Nedhuapal B.O",
      "Patbil B.O",
      "Rasamtala B.O",
      "San Deuli B.O",
      "Karanjia Bazar S.O",
      "Karanjia Court S.O",
      "Karanjia Golei S.O"
    ]
  },
  "757038": {
    "pincode": "757038",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Thakurmunda S.O",
      "Baunsdiha B.O",
      "Bharandia B.O",
      "Champajhar B.O",
      "Digdhar B.O",
      "Hatigoda B.O",
      "Jarak B.O",
      "Keshdiha B.O",
      "Khandabandh B.O",
      "Mahuldiha B.O",
      "Mituani B.O",
      "Padiabeda B.O",
      "Patarapada B.O",
      "Salchua B.O",
      "Saleibeda B.O",
      "Satkosia B.O",
      "Talpada B.O",
      "Taramara B.O"
    ]
  },
  "757039": {
    "pincode": "757039",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Sukruli S.O",
      "Arjunbilla B.O",
      "Badterenti B.O",
      "Bramarposi B.O",
      "Chaturanjali B.O",
      "Galusahi B.O",
      "Haldia B.O",
      "Jamunti B.O",
      "Jhadghosada B.O",
      "Khiching B.O",
      "Kumbhirda B.O",
      "Nuabeda B.O",
      "Pandarsil B.O",
      "Singada B.O",
      "Talgaon B.O"
    ]
  },
  "757040": {
    "pincode": "757040",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Kaptipada S.O",
      "Parasbani B.O",
      "Peragari B.O",
      "Sanbisole B.O",
      "Badgudugudia B.O",
      "Badkhaladi B.O",
      "Badsimulia B.O",
      "Chuinposi B.O",
      "Golagadia B.O",
      "Goudagaon B.O",
      "Jaida B.O",
      "Jambani B.O",
      "Kaladahi B.O",
      "Mankadapada B.O",
      "Mirigothana B.O"
    ]
  },
  "757041": {
    "pincode": "757041",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Udala S.O",
      "Ambadiha B.O",
      "Badsingaria B.O",
      "Bahubandh B.O",
      "Bhimtali B.O",
      "Dughdha B.O",
      "Jaida B.O",
      "Jualia B.O",
      "Kainsari B.O",
      "Khaladi B.O",
      "Kutling B.O",
      "Nayarangamatia B.O",
      "Patpur B.O",
      "Salmundali B.O",
      "Udala Bazar S.O",
      "Udala College S.O"
    ]
  },
  "757042": {
    "pincode": "757042",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Gorumahisani S.O",
      "Anlajo R.S. B.O",
      "Badgaon B.O",
      "Badmouda B.O",
      "Badra B.O",
      "Bhimkhanda B.O",
      "Guhaldangri B.O",
      "Hatia B.O",
      "Kukudimundi B.O",
      "Mahadevdihi B.O",
      "Sanpakhana B.O",
      "Sudarsanpur B.O",
      "Sundhal B.O",
      "Tikhia B.O"
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
