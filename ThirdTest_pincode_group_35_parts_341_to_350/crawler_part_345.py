"""
================================================================================
ALL-INDIA PIN CODE GOOGLE MAPS LEADS CRAWLER - SPLIT PART 345 / 400
================================================================================
- Group: ThirdTest_pincode_group_35_parts_341_to_350
- Assigned PIN Codes: 48 (Range: 757043 to 758023)
- Unique Categories: 256
- Total Search Combinations: 12,288 (Strict 12,288 scale!)
- Expected Run Duration: ~1 to 1.5 hours (Fast & Zero Timeout Risk)
- 4-Tier Output Folders (both CSV and JSON in all folders):
  1) master/                -> ALL_INDIA_LEADS_PART_345.csv & .json
  2) by_pincode/            -> <pincode>.csv & <pincode>.json
  3) by_category/           -> <category>.csv & <category>.json
  4) by_combination/        -> <pincode>_<category>.csv & .json
  5) pincode_city_reference/-> pincode_city_mapping_part_345.csv & .json
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

PART_ID = "part_345"
LEAD_AUTO_SAVE_THRESHOLD = 25000

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [Part-345] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(f"PincodeCrawler_{PART_ID}")

# Assigned PIN codes for this partition (48 PIN codes)
ASSIGNED_PINCODES = [
  "757043",
  "757045",
  "757046",
  "757047",
  "757048",
  "757049",
  "757050",
  "757051",
  "757052",
  "757053",
  "757054",
  "757055",
  "757073",
  "757074",
  "757075",
  "757077",
  "757079",
  "757081",
  "757082",
  "757083",
  "757084",
  "757085",
  "757086",
  "757087",
  "757091",
  "757092",
  "757093",
  "757100",
  "757101",
  "757102",
  "757103",
  "757104",
  "757105",
  "757106",
  "757107",
  "758001",
  "758002",
  "758013",
  "758014",
  "758015",
  "758016",
  "758017",
  "758018",
  "758019",
  "758020",
  "758021",
  "758022",
  "758023"
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
  "757043": {
    "pincode": "757043",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Rairangpur H.O",
      "Anladuva S.O",
      "Mahuldiha S.O",
      "Rairangpur Bazar S.O",
      "Rairangpur R.S S.O"
    ]
  },
  "757045": {
    "pincode": "757045",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Jamda S.O (Mayurbhanj)",
      "Badkuleibira B.O",
      "Halda B.O",
      "Jarkani B.O",
      "Khairpal B.O",
      "Pasana B.O",
      "Talgaon B.O",
      "Tarana B.O",
      "Tendra B.O"
    ]
  },
  "757046": {
    "pincode": "757046",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Bahalda S.O",
      "Basingi B.O",
      "Binjhua B.O",
      "Dundu B.O",
      "Gambharia B.O",
      "Indukhali B.O",
      "Kulgi B.O",
      "Moronda B.O",
      "Purunapani B.O",
      "Soso B.O",
      "Tamalbandh B.O"
    ]
  },
  "757047": {
    "pincode": "757047",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Badampahar S.O",
      "Basilapir B.O",
      "Dova B.O",
      "Jhipabandh B.O",
      "Jorda B.O",
      "Kathbharia B.O",
      "Padhia B.O",
      "Talakpokhari B.O"
    ]
  },
  "757048": {
    "pincode": "757048",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Bijatala S.O",
      "Ambadiha B.O",
      "Badamtalia B.O",
      "Banakati B.O",
      "Damudigoda B.O",
      "Luhakani B.O",
      "Luhasila B.O",
      "Manikpur B.O",
      "Raihari B.O",
      "Sargoda B.O"
    ]
  },
  "757049": {
    "pincode": "757049",
    "circle": "Odisha circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Balidiha B.O",
      "Bhaluki B.O",
      "Chandanpur B.O",
      "Chhadkata B.O",
      "Haldibani B.O",
      "Jagannathpur B.O",
      "Kendua B.O",
      "Shyamakhunta S.O",
      "Kisandahi B.O",
      "Kochilaghaty B.O",
      "Palpala Ambadali B.O",
      "Poda astia B.O",
      "Rangamatia B.O",
      "Saratchandrapur B.O",
      "Sindurgoura B.O",
      "Sirishbani B.O"
    ]
  },
  "757050": {
    "pincode": "757050",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Hatbadra S.O",
      "Aharbandh B.O",
      "Bhalubasa B.O",
      "Chuapani B.O",
      "Hesda B.O",
      "Kumudasole B.O",
      "Kusumi B.O",
      "Pratapgarh B.O",
      "Purunaghati B.O",
      "Purunapani B.O",
      "Shyamsundar B.O",
      "Suleipat B.O",
      "Ukam B.O",
      "Uparbeda B.O"
    ]
  },
  "757051": {
    "pincode": "757051",
    "circle": "Odisha circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Bagbuda B.O",
      "Kherna B.O",
      "Mahulbarei B.O",
      "Murunia B.O",
      "Palasbani B.O",
      "Pokharia B.O",
      "Ratila B.O",
      "Silfodi B.O",
      "Unchagaon B.O",
      "Saraskana S.O"
    ]
  },
  "757052": {
    "pincode": "757052",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Jugpura S.O",
      "Aldia Bartana B.O",
      "Anla B.O",
      "Bhudurbani B.O",
      "Durgapur B.O",
      "Hatijhari B.O",
      "Jugal B.O",
      "Khadikapada B.O",
      "Machhapada B.O",
      "Merda B.O",
      "Paika Sahi B.O",
      "Raghavpur B.O",
      "Raikama B.O",
      "Saitpur B.O",
      "Sansa B.O",
      "Tilapada B.O",
      "Totapada B.O",
      "KURADHIKA B.O."
    ]
  },
  "757053": {
    "pincode": "757053",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Tiring S.O",
      "Baddalima B.O",
      "Badpalasa B.O",
      "Bhagabandi B.O",
      "Changua B.O",
      "Dharamdihi B.O",
      "Jirei B.O",
      "Lupung B.O",
      "Maghua B.O",
      "Pandupani B.O",
      "Sanbhundu B.O"
    ]
  },
  "757054": {
    "pincode": "757054",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Bahalda Rd RS S.O",
      "Asana B.O",
      "Badkedam B.O",
      "Bhitaramda B.O",
      "Dolsara B.O",
      "Soroda Jashipur B.O",
      "Tarana B.O"
    ]
  },
  "757055": {
    "pincode": "757055",
    "circle": "Odisha circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Brahmapura B.O",
      "Haripur B.O",
      "Mangalpur B.O",
      "Panisapada B.O",
      "Purnachandrapur B.O",
      "Amarda S.O",
      "Sarisa B.O"
    ]
  },
  "757073": {
    "pincode": "757073",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Nalagaja S.O",
      "Bhaduasole B.O",
      "Harekrushnapur B.O",
      "Khuruntia B.O",
      "Mahulia B.O",
      "Rangiam B.O",
      "Tukpalasia B.O"
    ]
  },
  "757074": {
    "pincode": "757074",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Basipitha S.O",
      "Badfeni B.O",
      "Badpathara B.O",
      "Bahalda B.O",
      "Bangra B.O",
      "Sapanchua B.O",
      "Talakunda B.O",
      "Tangana B.O",
      "BANDHAGODA"
    ]
  },
  "757075": {
    "pincode": "757075",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Dukura S.O",
      "Ambadali B.O",
      "Astajharan B.O",
      "Bireswarpur B.O",
      "Chandrapur B.O",
      "Itamundia B.O",
      "Kamalasole B.O",
      "Kamali B.O",
      "Khanua B.O",
      "Lakhanasahi B.O",
      "Narankhunta B.O",
      "Salbani B.O"
    ]
  },
  "757077": {
    "pincode": "757077",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Nudadiha S.O",
      "Badkhaman B.O",
      "Chakradharpur B.O",
      "Hill Block   24 B.O",
      "Kolialam B.O",
      "Manikpur B.O",
      "Podadiha B.O",
      "Potaldiha B.O",
      "Sarbanghati B.O"
    ]
  },
  "757079": {
    "pincode": "757079",
    "circle": "Odisha circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Dewan Bahali B.O",
      "Gour Chandra Pur B.O",
      "Kalamgodia B.O",
      "Katuria B.O",
      "Labanyadeipur B.O",
      "Nedam B.O",
      "Rajat Nagar B.O",
      "Saradiha B.O",
      "Sarat S.O"
    ]
  },
  "757081": {
    "pincode": "757081",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Kumbharmundakata S.O",
      "Budama R.S. B.O",
      "Dhangudisole B.O",
      "Dudhiasole B.O",
      "Kesharpur B.O",
      "Khairbani B.O",
      "Kothabila B.O",
      "Nodhana B.O",
      "Sankucha B.O"
    ]
  },
  "757082": {
    "pincode": "757082",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Garhdeulia S.O",
      "Alda B.O",
      "Balka B.O",
      "Durgapur B.O",
      "Gosaingaon B.O",
      "Hempur B.O",
      "Kalama B.O",
      "Kantipur B.O",
      "Palasia B.O"
    ]
  },
  "757083": {
    "pincode": "757083",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Bhimda S.O",
      "Arpata B.O",
      "C.Mangalpur B.O",
      "Daundia B.O",
      "Patisari B.O",
      "Tentuligaon B.O"
    ]
  },
  "757084": {
    "pincode": "757084",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Kostha S.O",
      "Badbhalia B.O",
      "Baddhenkia B.O",
      "Brahmanmara B.O",
      "Chadheigaon B.O",
      "Dharampura B.O",
      "Fania B.O",
      "Gouduniduba B.O",
      "Kaduani B.O",
      "Marudihi B.O",
      "Naupada B.O",
      "Nuhajhalia B.O",
      "Paktia B.O"
    ]
  },
  "757085": {
    "pincode": "757085",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Kushalda S.O",
      "Baradihi B.O",
      "Debgaon B.O",
      "Jaipur B.O",
      "Pathurikata B.O",
      "Srinathpur B.O",
      "Taradasole B.O"
    ]
  },
  "757086": {
    "pincode": "757086",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Jharpokharia S.O",
      "Badchatra B.O",
      "Badsole B.O",
      "Belbaria B.O",
      "Bhursani B.O",
      "Jalda B.O",
      "Khadiasole B.O",
      "Kuanrdihi B.O",
      "Nuhamalia B.O",
      "Padmapur Deuli B.O",
      "Rajaloka B.O"
    ]
  },
  "757087": {
    "pincode": "757087",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Brundaban Chandrapur S.O",
      "Ambikadeipur B.O",
      "Athapara B.O",
      "Balichhatra B.O",
      "Chadada B.O",
      "Chandrapur B.O",
      "Dhanghera B.O",
      "Gadigaon B.O",
      "Hatisahi B.O",
      "Kundabai B.O",
      "Patsanipur B.O",
      "Sridamchandrapur B.O"
    ]
  },
  "757091": {
    "pincode": "757091",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Khairi Jashipur S.O",
      "Bakla B.O",
      "Begunia B.O",
      "Bhanjkia B.O",
      "Ektali B.O",
      "Gadapalasa B.O",
      "Jharbeda B.O",
      "M.Beuinria B.O",
      "Matiagarh B.O",
      "Neuti B.O",
      "Sirakuli B.O",
      "Tilakothi B.O"
    ]
  },
  "757092": {
    "pincode": "757092",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Kusumbandi S.O",
      "Badgaon B.O",
      "Bhuasuni B.O",
      "Budhikhamari B.O",
      "Hinjili B.O",
      "Jaldiha B.O",
      "Kanchhinda B.O",
      "Kundulia B.O",
      "Labania B.O",
      "Sarisapal B.O",
      "Shyamsundarpur B.O"
    ]
  },
  "757093": {
    "pincode": "757093",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Joka S.O (Mayurbhanj)",
      "Dumurdiha B.O",
      "Paktia B.O",
      "Pokhardiha B.O",
      "Pokpoka B.O",
      "Sankhabhanga B.O"
    ]
  },
  "757100": {
    "pincode": "757100",
    "circle": "Odisha circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Badchhuruni B.O",
      "Bahanada Sathilo S.O",
      "Dahikoti B.O",
      "Muktapur B.O",
      "Purikhunta B.O",
      "Purinda B.O",
      "Saria B.O",
      "Tarakothi B.O"
    ]
  },
  "757101": {
    "pincode": "757101",
    "circle": "Odisha circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Bairatpur B.O",
      "Dihirakul B.O",
      "Dimagadia B.O",
      "Teldihudi B.O",
      "Radho S.O"
    ]
  },
  "757102": {
    "pincode": "757102",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Puruna Baripada S.O",
      "Chaturi B.O",
      "Pasuda B.O",
      "Sainkula B.O",
      "Sanbelakuti B.O",
      "Sirathali B.O"
    ]
  },
  "757103": {
    "pincode": "757103",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Pratappur S.O (Mayurbhanj)",
      "Haripur B.O",
      "Madhopur B.O",
      "Orachandbilla B.O"
    ]
  },
  "757104": {
    "pincode": "757104",
    "circle": "Odisha circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Bholagadia B.O",
      "Dengam B.O",
      "Gayalmara B.O",
      "Bahanada S.O",
      "Nuagaon B.O"
    ]
  },
  "757105": {
    "pincode": "757105",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Kuchei S.O",
      "Aniapal B.O",
      "Asanjoda B.O",
      "Baiganbaria B.O",
      "Dumurdiha B.O",
      "Gangraj B.O",
      "Haldia (Baripada) B.O",
      "Kantapal B.O"
    ]
  },
  "757106": {
    "pincode": "757106",
    "circle": "Odisha circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Mahulpankha B.O",
      "Majhigadia B.O",
      "Padmapokhari B.O",
      "Raipal B.O",
      "Sundhal B.O",
      "Salchua S.O"
    ]
  },
  "757107": {
    "pincode": "757107",
    "circle": "Odisha Circle",
    "region": "Bhubaneswar HQ Region",
    "division": "Mayurbhanj Division",
    "offices": [
      "Laxmiposi S.O",
      "Bankisole B.O",
      "Bhudrubani B.O",
      "Garudnesa B.O",
      "Kainfulia B.O",
      "Patharchakuli B.O",
      "Rajabasa B.O",
      "Sanfutuka B.O",
      "Sankhabhanga B.O"
    ]
  },
  "758001": {
    "pincode": "758001",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Keonjhargarh H.O",
      "Keonjhar Court S.O",
      "Keonjhar Mining School S.O",
      "Keonjhar New Markt S.O",
      "Keonjhar Science College S.O",
      "Madhapur S.O"
    ]
  },
  "758002": {
    "pincode": "758002",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Keonjhar Bazar S.O",
      "Bodapalasa B.O",
      "Ghutur B.O",
      "Nelung B.O",
      "Ranki B.O",
      "Keonjhar Atopur S.O"
    ]
  },
  "758013": {
    "pincode": "758013",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Raisuan S.O",
      "Bauripada B.O",
      "Birakishorepur B.O",
      "Dhrupada B.O",
      "Gobardhan B.O",
      "Kadagarh B.O",
      "Kempasada B.O",
      "Kusumita B.O",
      "Mahadeijoda B.O",
      "Nuagaon B.O",
      "Padmapur B.O",
      "Sankir B.O",
      "Sendkap B.O",
      "Tikarpada B.O"
    ]
  },
  "758014": {
    "pincode": "758014",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Naranpur S.O",
      "Badaposi B.O",
      "Bolaniposi B.O",
      "Dimba B.O",
      "Dimirimunda B.O",
      "Haladharpur B.O",
      "Kudipasa B.O",
      "Maidankela B.O",
      "Mandua B.O",
      "Raghunathpur B.O",
      "Sirishpal B.O"
    ]
  },
  "758015": {
    "pincode": "758015",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Ghasipura S.O",
      "Atasahi B.O",
      "Bada Ektali B.O",
      "Biragobindapur B.O",
      "Dhakotha B.O",
      "Gohira B.O",
      "Kanto B.O",
      "Khaliamenta B.O",
      "Kolimati B.O",
      "Naduan B.O",
      "Sailong B.O",
      "Santoshpur B.O",
      "Trilochanpur B.O"
    ]
  },
  "758016": {
    "pincode": "758016",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Saharpada S.O",
      "Badabaliposi B.O",
      "Baratania B.O",
      "Begna B.O",
      "Bholapada B.O",
      "Digiposi B.O",
      "Gujitangiri B.O",
      "Kapundi B.O",
      "Raidiha B.O",
      "Tanda B.O",
      "Tavasarua B.O",
      "Tendra B.O"
    ]
  },
  "758017": {
    "pincode": "758017",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Rajanagar S.O",
      "Badagambharia B.O",
      "Chakundapal B.O",
      "Jadichatar B.O",
      "Jamunaposi B.O",
      "Kantiapada B.O",
      "Kendeiposi B.O",
      "Murusuan B.O",
      "Silida B.O"
    ]
  },
  "758018": {
    "pincode": "758018",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Suakati S.O",
      "Barhagarh B.O",
      "Bayakumutia B.O",
      "Champei B.O",
      "Guptaganga B.O",
      "Kanjipani B.O",
      "Kuanr B.O",
      "Pandadar B.O"
    ]
  },
  "758019": {
    "pincode": "758019",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Telkoi S.O",
      "Deoladiha B.O",
      "Doblapal B.O",
      "Goda B.O",
      "Jata B.O",
      "Karadangi B.O",
      "Khuntapada Charigarh B.O",
      "Oriya B.O",
      "Talapada B.O"
    ]
  },
  "758020": {
    "pincode": "758020",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Salapada S.O",
      "Bailo B.O",
      "Belabahali B.O",
      "Panchupally B.O",
      "Purunia B.O",
      "Tolankapada B.O",
      "Tukuna B.O"
    ]
  },
  "758021": {
    "pincode": "758021",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Anandapur S.O (Kendujhar)",
      "Angarua B.O",
      "Badapadana B.O",
      "Baladuan B.O",
      "Bankhidi B.O",
      "Baunsagarh B.O",
      "Gayalamunda B.O",
      "Kantipal B.O",
      "Kathakata B.O",
      "Kodapada B.O",
      "Manoharpur B.O",
      "Nuagaon B.O",
      "Padmapur B.O",
      "Panasadiha B.O",
      "Purunaghati B.O",
      "Raitola B.O",
      "Salabani B.O",
      "Taneipal B.O",
      "Taratara B.O",
      "Anandapur College S.O"
    ]
  },
  "758022": {
    "pincode": "758022",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Fakirpur S.O",
      "Ambagadia B.O",
      "Ambo B.O",
      "Bancho B.O",
      "Biridi B.O",
      "Girigaon B.O",
      "Jambhara B.O",
      "Karagola B.O",
      "Mugupur B.O",
      "Samana B.O",
      "Sankho B.O",
      "Sulana B.O"
    ]
  },
  "758023": {
    "pincode": "758023",
    "circle": "Odisha Circle",
    "region": "Sambalpur Region",
    "division": "Keonjhar Division",
    "offices": [
      "Hadagarh S.O",
      "Balibarei B.O",
      "Bangore B.O",
      "Dhenka B.O",
      "Podasingidi B.O",
      "Sadha B.O"
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
