import os
import time
import nltk
import requests
from elastic import get_elastic_client
from elasticsearch import NotFoundError
nltk.download('punkt')


# based on 2000 calories diet
daily_values = {
    "fat": { "gram": 65 },
    "transFat": { "gram": 13 },
    "polyunsaturatedFat": { "gram": 13 },
    "monounsaturatedFat": { "gram": 13 },
    "saturatedFat": { "gram": 20 },
    "cholesterol": { "milligram": 300 },
    "sodium": { "milligram": 2400 },
    "carbohydrates": { "gram": 300 },
    "sugars": { "gram": 50 },
    "addedSugar": { "gram": 20 },
    "fiber": { "gram": 30 },
    "protein": { "gram": 65 },
    "alcohol": { "gram": 14 },
    "iron": { "milligram": 18 },
    "calcium": { "milligram": 1000 },
    "potassium": { "milligram": 4700 },
    "vitamin A": { "iu": 5000, "microgram": 1500 },
    "vitamin C": { "milligram": 60 },
    "vitamin D": { "iu": 800, "microgram": 20 },
    "vitamin E": { "milligram": 15 },
    "vitamin K": { "microgram": 120 },
    "thiamin": { "milligram": 1.2 },
    "riboflavin": { "milligram": 1.3 },
    "niacin": { "milligram": 16 },
    "vitamin B6": { "milligram": 1.3 },
    "folate": { "microgram": 400 },
    "vitamin B12": { "microgram": 2.4 },
    "pantothenicAcid": { "milligram": 5 },
    "biotin": { "microgram": 30 },
    "choline": { "milligram": 550 },
    "phosphorus": { "milligram": 1000 },
    "magnesium": { "milligram": 400 },
    "zinc": { "milligram": 11 },
    "copper": { "milligram": 0.9 },
    "manganese": { "milligram": 2.3 },
    "selenium": { "microgram": 55 },
    "chromium": { "microgram": 35 },
    "molybdenum": { "microgram": 45 },
    "iodine": { "microgram": 150 },
    "vitamin B1": { "milligram": 1.2 },
    "vitamin B2": { "milligram": 1.3 },
    "vitamin B3": { "milligram": 16 },
    "vitamin B5": { "milligram": 5 },
}


def convert_iu(field, iu):

    if daily_values.get(field):
        for unit, val in daily_values[field].items():
            if unit != "iu":
                return unit, val * iu / 100.00
    else:
        print(f"Failed to find: {field} in daily_values")
        return field, iu


def convert_units(nutrient, from_unit, to_unit, value):
    if from_unit.lower() in ["g", "gram"]:
        from_unit = "gram"
    elif from_unit.lower() in ["mg", "milligram"]:
        from_unit = "milligram"
    elif from_unit.lower() in ["mcg", "microgram"]:
        from_unit = "microgram"
    elif from_unit.lower() in ["iu", "ius"]:
        from_unit = "iu"
    elif from_unit.lower() in ["percentage", "percentages"]:
        from_unit = "percent"

    if to_unit.lower() in ["g", "gram"]:
        to_unit = "gram"
    elif to_unit.lower() in ["mg", "milligram"]:
        to_unit = "milligram"
    elif to_unit.lower() in ["mcg", "microgram"]:
        to_unit = "microgram"

    if nutrient in daily_values.keys():
        if from_unit == "iu" and to_unit in ["mcg", "mg"]:
            new_unit, new_value = convert_iu(nutrient, value)
            return convert_units(nutrient, new_unit, to_unit, new_value)
        elif from_unit == "milligram" and to_unit == "gram":
            new_value = value / 1000
            return to_unit, new_value
        elif from_unit == "gram" and to_unit == "milligram":
            new_value = value * 1000
            return to_unit, new_value
        elif from_unit == "microgram" and to_unit == "milligram":
            new_value = value / 1000
            return to_unit, new_value
        elif from_unit == "milligram" and to_unit == "microgram":
            new_value = value * 1000
            return to_unit, new_value
        elif from_unit == "microgram" and to_unit == "gram":
            new_value = value / 1000000
            return to_unit, new_value
        elif from_unit == "gram" and to_unit == "microgram":
            new_value = value * 1000000
            return to_unit, new_value
        elif from_unit == "percent":
            new_value = value * daily_values[nutrient][to_unit] / 100
            return to_unit, new_value
        elif from_unit in daily_values[nutrient] and to_unit in daily_values[nutrient]:
            # there shouldn't be a case of these
            conversion_factor = daily_values[nutrient][to_unit] / daily_values[nutrient][from_unit]
            if value:
                try:
                    if type(value) == str:
                        if value.endswith("%"):
                            new_value = float(value.strip("%")) * daily_values[nutrient][to_unit]
                        elif value == "na":
                            new_value = None
                        else:
                            new_value = float(value) * conversion_factor
                    else:
                        new_value = value * conversion_factor
                except:
                    print(f"{nutrient} {from_unit} {to_unit} {value}")
            else: 
                new_value = None
            return to_unit, new_value
        elif not from_unit:
            # get default units from daily_value
            new_unit, _ = next(iter(daily_values[nutrient].items()))
            if new_unit == to_unit:
                return to_unit, value
            else:
                return convert_units(nutrient, new_unit, to_unit, value)
                  
        else:
            print(f"Invalid units for nutrient: {nutrient} - {from_unit} to {to_unit}")
    else:
        print(f"Failed to find nutrient: {nutrient} in daily_values")

    # Return the original values if conversion fails
    return from_unit, value


g   = ["protein", "fiber", "sugars", "addedSugar", "carbohydrates", "saturatedFat", "monounsaturatedFat", "polyunsaturatedFat", "fat", "transFat"]
mg  = ["manganese", "copper", "zinc", "magnesium", "phosphorus", "calcium", "choline", "pantothenicAcid", "vitamin B", "vitamin B1", 
       "vitamin B2", "vitamin B3", "vitamin B5", "vitamin B6", "niacin", "riboflavin", "thiamin", "vitamin C", "vitamin E", 
       "potassium", "calcium", "iron", "sodium", "cholesterol", "water" ]
mcg = ["iodine", "molybdenum", "chromium", "selenium", "biotin", "vitamin B12", "folate", "vitamin K", "vitamin D", "vitamin A", "coffeine", 
       "inositol", "choline", "inulin", "tryptophan", "threonine", "isoleucine", "leucine", "lysine", "methionine", "cystine", "phenylalanine",
        "tyrosine", "valine", "arginine", "histidine", "alanine", "glycine", "proline", "aspartic acid", "glutamic acid", "ash" ]
iu  = ["vitamin D", "vitamin A" ]

# list of superfoods
superfoods =  [ "blueberries", "sweet potatoes", "salmon", "broccoli", "spinach", "avocado", "egg", "quinoa", "almonds", "chia seeds",
                "turmeric", "pumpkin", "garlic", "green tea", "kale", "bell peppers", "grapefruit", "watermelon", "flax seeds", 
                "mushrooms", "papaya", "apples", "ginger", "lentils", "collard greens", "beets", "oats", "carrots", "cabbage", 
                "cauliflower", "brussels sprouts", "plums", "artichokes", "peas", "brown rice", "buckwheat", "peanuts", "watercress",
                "asparagus", "radishes", "figs", "pomegranate", "black beans", "sunflower seeds", "pineapple", "raspberries", "tofu",
                "cantaloupe", "walnuts", "mango", "cashews", "oranges", "cherry", "strawberries", "blackberries", 
                "papaya", "dates", "pears", "nectarines", "grapes", "persimmons", "kiwi", "peaches", "apricots", "lemon", 
                "limes", "grapefruits", "tangerines", "clementines", "pomegranates", "strawberries", "cranberries",
                "elderberries", "cherries", "melons", "mangoes", "guava", "brazilian nuts", "seaweed", "pistachios", 
                "macademia nuts", "peanuts", "tuna", "mackerel", "sardines", "trout", "herring", "shiitake mushrooms",
                "maitake mushrooms", "oyster mushrooms", "reishi mushrooms", "chaga mushrooms", "cordyceps mushrooms" ]


def get_food_info(fdcid):
    
    usda_api_key = os.environ.get("USDA_API_KEY")
    url = f"https://api.nal.usda.gov/fdc/v1/food/{fdcid}?nutrients=203&nutrients=204&nutrients=205&api_key={usda_api_key}"
    response = requests.get(url)
    
    # api allows for 1000 calls per hour (max)
    time.sleep(4)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Request failed with status code: {response.status_code}")
        return None

def fix_recipe_units(nutrition_labels):
    nutrients = {}
    for field, value in nutrition_labels.items():
        if type(value) == dict:
            if len(value.items())>1:
                subfield, subval = next(iter(value.items()))
            else:
                # most likely {}
                if value and not value == "na":
                    subfield, subval = "", next(iter(value.values()))
                else:
                    subfield = ""
                    subval = None
        else:
            subfield = ""
            if value and not value == "na":
                subval = value
            else:
                subval = None
        
        if field in g:
            if subfield not in ['gram', 'grams', 'g']:
                #print(f"{field} - unit is {subfield} - should be grams")
                unit, uval = convert_units(field, subfield, 'g', subval)
                if uval and not uval == "na":
                    new_val = { "amount": uval, "unit_name": unit }
                else:
                    new_val = { "amount": None, "unit_name": unit }
            else:
                if subval and not subval == "na":
                    new_val = { "amount": subval, "unit_name": "g" }
                else:
                    new_val = { "amount": None, "unit_name": "g" }
        
        elif field in mg:
            if subfield not in ['milligram', 'milligrams', 'mg']:
                #print(f"{field} - unit is {subfield} - should be milligrams")
                unit, uval = convert_units(field, subfield, 'mg', subval)
                if uval and not uval == "na":
                    new_val = { "amount": uval, "unit_name": unit }
                else:
                    new_val = { "amount": None, "unit_name": unit }
            else:
                if subval and not subval == "na":
                    new_val = { "amount": subval, "unit_name": "mg" }
                else:
                    new_val = { "amount": None, "unit_name": "mg" }

        elif field in iu:
            if not field in mg and not field in mcg:
                # in IUs - need to convert to mcg for consistency 
                if subfield in ['IU', 'percentage', 'IUs', 'iu', 'percentages']:
                    unit, uval = convert_units(field, subfield, 'mcg', subval)
                    if uval and not uval == "na":
                        new_val = { "amount": uval, "unit_name": unit }
                    else:
                        new_val = { "amount": None, "unit_name": unit }
                else:
                    print(f"{field} - unit is {subfield}")
                    if subval and not subval == "na":
                        new_val = { "amount": subval, "unit_name": "mcg" }
                    else:
                        new_val = { "amount": None, "unit_name": "mcg" }

            elif field in mcg and not field in mg:
                # should be in micrograms
                if subfield in ['microgram', 'mcg']:
                    new_val = { "amount": subval, "unit_name": "mcg" }
                else:
                    # print(f"{field} - unit is {subfield} - should be micrograms")
                    unit, uval = convert_units(field, subfield, 'mcg', subval)
                    if uval and not uval == "na":
                        new_val = { "amount": uval, "unit_name": unit }
                    else:
                        new_val = { "amount": None, "unit_name": unit }
            else:
                # convert to mcg for consistency
                if subval and not subval == 'na':
                    unit, uval = convert_units(field, subfield, 'mcg', subval)
                    new_val = { "amount": uval, "unit_name": unit }
                else:
                    new_val = { "amount": None, "unit_name": unit }

        elif field in ["calories", "caloriesFromFat"]:
            if subval and not subval == 'na':
                new_val = { "amount": subval, "unit_name": "kcal" }
            else:
                new_val = { "amount": None, "unit_name": "kcal" }

        elif field in ["servings", "servingSize"]:
            if subval and not subval == 'na':
                new_val = { "amount": subval, "unit_name": "" }
            else:
                new_val = { "amount": None, "unit_name": "" }
        else:
            print(f"Unrecognized field: {field}")
            if subval and not subval == 'na':
                new_val = { "amount": subval, "unit_name": subfield if subfield else "" }
            else:
                new_val = { "amount": None, "unit_name": subfield if subfield else "" }

        nutrients[field] = new_val
    
    return nutrients


def fix_recipes_labels(nutrition_labels):
    # {"name": "Arroz con Pollo (Chicken with Rice)", 
    # "link": "https://diabetes.extension.illinois.edu/arroz-con-pollo-chicken-rice", 
    # "photo": "https://diabetes.extension.illinois.edu/sites/default/files/2019-09/337_1sm.jpg", 
    # "description": "Arroz con Pollo (Chicken with Rice)", 
    # "ingredients": {"4 skinless, boneless chicken breast halves (about 1.2 pounds)": {"chicken": "4.0", "pound": "1.2"}, "0.5 teaspoon salt": {"teaspoon": "0.5"}, "1 cup frozen green bell pepper, chopped": {"cup": "1.0", "veggies": "bell pepper, pepper, bell pepper"}, "1 0.5 teaspoons minced garlic": {"teaspoon": "1.5", "veggies": "garlic"}, "1 cup long-grain white rice": {"cup": "1.0", "rice": "white rice", "grains": "rice"}, "1 14.5-ounce can chicken broth": {"can, chicken": "14.5"}, "0.5 cup white wine": {"cup": "0.5"}, "1 14.5-ounce can stewed tomatoes": {"can, tomatoes": "14.5", "veggies": "tomatoes", "fruits": "tomatoes"}, "1 cup onion pearls": {"cup": "1.0", "veggies": "onion"}}, 
    # "nutrients": {"servings": {"na": 9.0}, 
    #               "calories": {"kcal": 218.0}, 
    #               "calories_from_fat": {"kcal": 22.0}, 
    #               "total_fat": {"gram": 2.0}, 
    #               "cholesterol": {"milligram": 35.0}, 
    #               "sodium": {"milligram": 827.0}, 
    #               "dietary_fiber": {"gram": 1.0}, 
    #               "protein": {"gram": 1.0}}, 
    # "prep_time": "15", 
    # "cook_time": "75", 
    # "total_time": "75", 
    # "calories": "218", 
    # "servings": 9.0, 
    # "directions": {"1": "Cut each breast into 1-inch pieces. Sprinkle chicken with 1/4 teaspoon salt.", "2": "Heat large skillet and add chicken. Cook until golden. Remove chicken, and set aside.", "3": "Add green pepper, onions, and garlic to skillet. Cook for 5 minutes.", "4": "Add rice; cook and stir until rice is opaque, 1 to 2 minutes.", "5": "Stir in broth, white wine, and tomatoes, followed by remaining salt, pepper, and paprika. Return to a boil. Cover, and simmer for 20 minutes.", "6": "Return chicken to the skillet, and cook about 5 minutes until chicken is reheated."}, 
    # "tips": {}, 
    # "source": "diabetes.extension.illinois.edu"}
    fieldmap = { 
        "servings": "servings",
        "calories": "calories",
        "calories_from_fat": "caloriesFromFat",
        "saturated_fat": "saturatedFat",
        "polyunsaturated_fat": "polyunsaturatedFat",
        "monounsaturated_fat": "monounsaturatedFat",
        "total_fat": "fat",
        "trans_fat": "transFat",
        "Vitamin A": "vitamin A",
        "Vitamin C": "vitamin C",
        "Vitamin B": "vitamin B",
        "Vitamin D": "vitamin D",
        "vitamin A": "vitamin A",
        "vitamin C": "vitamin C",
        "vitamin B": "vitamin B",
        "vitamin D": "vitamin D",
        "cholesterol": "cholesterol",
        "sodium": "sodium",
        "potassium": "potassium",
        "iron": "iron",
        "calcium": "calcium",
        "Protein": "protein",
        "protein": "protein",
        "total_carbs": "carbohydrates",
        "Cholesterol": "cholesterol",
        "Energy": "calories",
        "sugars": "sugars",
        "added_sugar": "addedSugar",
        "added_sugars": "addedSugar", 
        "total_sugars": "sugars",
        "dietary_fiber": "fiber",
        "Fiber, total dietary": "fiber",
        "servings": "servings",
        "serving_size": "servingSize"
    }
    nutrients = {}
    for nutrient, values in nutrition_labels.items():
        if fieldmap.get(nutrient):
            new_field = fieldmap.get(nutrient)
            nutrients.update(fix_recipe_units({new_field: values}))
        else:
            print(f"{nutrient} - not found")
            nutrients.update(fix_recipe_units({nutrient: values}))
    
    return nutrients


def fix_nutrition_labels(nutrition_labels):

    # {'fat': {'value': 0.3}, 'saturatedFat': {'value': 0.0}, 'transFat': {'value': 0.0}, 
    # 'cholesterol': {'value': 0.0}, 'sodium': {'value': 43.0}, 'carbohydrates': {'value': 4.2}, 
    # 'fiber': {'value': 34.6}, 'sugars': {'value': 4.2}, 'protein': {'value': 4.3}, 'calcium': {'value': 0.0}, 
    # 'iron': {'value': 0.0}, 'potassium': {'value': 0.0}, 'calories': {'value': 37.0}}

    # calories: kcal: 23.0/ntotal_fat: gram: 0.0/nsaturated_fat: gram: 0.0/ncholesterol: milligram: 0.0/n
    # sodium: milligram: 68.0/ndietary_fiber: gram: 1.0/nprotein: gram: 0.0/ntotal_carbs: gram: 4.0/n
    # potassium: milligram: 18.0/nVitamin A: percentage: 4.0/nVitamin C: percentage: 10.0/n
    # calcium: percentage: 2.0/niron: percentage: 2.0/n",
    fieldmap = { 
        "Total lipid (fat)": "fat",
        "total_fat": "fat",
        "saturated_fat": "saturatedFat",
        "dietary_fiber": "fiber",
        "Protein": "protein",
        "total_carbs": "carbohydrates",
        "Carbohydrate, by difference": "carbohydrates",
        "Carbohydrate, other": "carbohydrates",
        "Cholesterol": "cholesterol",
        "Choline": "choline",
        "Choline, total": "choline",
        "Cystine": "cystine",
        "Chlorine, Cl": "chlorine",
        "Chromium, Cr": "chromium",
        "Energy": "calories",
        "Sugars, total including NLEA": "sugars",
        "Sugars, added": "addedSugar",
        "Alcohol, ethyl": "alcohol",
        "Arginine": "arginine", 
        "Alanine": "alanine",
        "Histidine": "histidine", 
        "Alanine": "alanine",
        "Fructose": "fructose",
        "Fiber, total dietary": "fiber",
        "Fiber, soluble": "fiber",
        "Fiber, insoluble": "fiber",
        "Calcium, Ca": "calcium",
        "Iron, Fe": "iron",
        "Iodine, I": "iodine",
        "Inulin": "inulin",
        "Inositol": "inositol",
        "Copper, Cu": "copper",
        "Lactose": "lactose",
        "Glucose": "glucose",
        "Zinc, Zn": "zinc",
        "Potassium, K": "potassium",
        "Thiamin": "thiamin",
        "Tyrosine": "tyrosine",
        "Sodium, Na": "sodium",
        "Selenium, Se": "sodium",
        "Riboflavin": "riboflavin",
        "Niacin": "niacin",
        "Magnesium": "magnesium",
        "Methionine": "methionine",
        "Molybdenum, Mo": "molybdenum",
        "Magnesium, Mg": "magnesium",
        "Manganese, Mn": "manganese",
        "Phosphorus, P": "phosphorus",
        "Pantothenic acid": "pantothenicAcid",
        "Phenylalanine": "phenylalanine",
        "Ash": "ash",
        "Water": "water",
        "Aspartic acid": "asparticAcid",
        "Acetic acid": "aceticAcid",
        "Lactic acid": "lacticAcid",
        "Glutamic acid": "glutamicAcid",
        "Glutamine": "glutamine",
        "Glycine": "glycine",
        "Folic acid": "folicAcid",
        "Folate, total": "folate",
        "Valine": "valine",
        "Vitamin A, IU": "vitamin A",
        "Vitamin B, IU": "vitamin B",
        "Vitamin B-1, IU": "vitamin B1",
        "Vitamin B-2, IU": "vitamin B2",
        "Vitamin B-3, IU": "vitamin B3",
        "Vitamin B-5, IU": "vitamin B5",
        "Vitamin B-6": "vitamin B6",
        "Vitamin B-12": "vitamin B12",
        "Vitamin C, total ascorbic acid": "vitamin C",
        "Vitamin D, IU": "vitamin D",
        "Vitamin D (D2 + D3), International Units": "vitamin D",
        "Vitamin D (D2 + D3)": "vitamin D",
        "Vitamin D3, IU": "vitamin D3",
        "Vitamin D3 (cholecalciferol)": "vitamin D3",
        "Vitamin E": "vitamin E",
        "Vitamin E (label entry primarily)": "vitamin E",
        "Vitamin E (alpha-tocopherol)": "vitamin E",
        "Vitamin K, (phylloquinone)": "vitamin K",
        "Vitamin K (phylloquinone)": "vitamin K",
        "Carotene, beta": "carotene",
        "Biotin": "biotin",
        "Folate, DFE": "folate",
        "Fluoride, F": "fluoride",
        "Ribose": "ribose",
        "SFA 8:0": "sfa",
        "SFA 10:0": "sfa",
        "SFA 12:0": "sfa",
        "PUFA 18:2": "pufa",
        "PUFA 18:2 n-6 c,c": "pufa",
        "PUFA 18:3 n-3 c,c,c (ALA)": "pufa",
        "Epigallocatechin-3-gallate": "gallate",
        "Caffeine": "caffeine",
        "Cystine": "cystine",
        "Cysteine": "cystine",
        "Sorbitol": "sorbitol",
        "Starch": "starch",
        "Xilitol": "xilitol",
        "Xylitol": "xylitol",
        "Lutein + zeaxanthin": "lutein",
        "Taurine": "taurine",
        "Tryptophan": "tryptothan",
        "Threonine": "threonine",
        "Isoleucine": "isoleucine",
        "Leucine": "leucine",
        "Lignin": "lignin",
        "Lysine": "lysine",
        "Proline": "proline",
        "Serine": "serine",
        "Total sugar alcohols": "alcohol",
        "Fatty acids, total trans": "transFat",
        "Fatty acids, total saturated": "saturatedFat",
        "Fatty acids, total monounsaturated": "monounsaturatedFat",
        "Fatty acids, total polyunsaturated": "polyunsaturatedFat"
    }

    nutrients = {}
    for nutrient, values in nutrition_labels.items():
        if fieldmap.get(nutrient):
            new_field = fieldmap.get(nutrient)
        else:
            print(f"{nutrient} - not found")
        if values:
            nutrients.update({new_field: values})
        else:
            nutrients.update({new_field: None})

    return nutrients

def fix_units(nutrition_labels):
    nutrients = {}
    for field, value in nutrition_labels.items():
        if field in g:
            new_val = { "amount": value.get("value"), "unit_name": "g" }
        elif field in mg:
            if field in iu:
                unit, new_val = convert_iu(field, value.get("value"))
                new_val = { "amount": value.get("value"), "unit_name": unit }
            else:
                new_val = { "amount": value.get("value"), "unit_name": "mg" }
        elif field in iu:
            if field not in mg:
                new_val = { "amount": value.get("value"), "unit_name": "IU" }
            else:
                unit, new_val = convert_iu(field, value.get("value"))
                new_val = { "amount": value.get("value"), "unit_name": unit }
        elif field in ["calories"]:
            new_val = { "amount": value.get("value"), "unit_name": "kcal" }
        else:
            print(f"Unrecognized field: {field}")
            new_val = { "amount": value.get("value"), "unit_name": "" }
        
        nutrients[field] = new_val
    
    return nutrients

def check_if_id_is_in_es_index(index_name,fdc_id):
    # Create an instance of the Elasticsearch client
    client = get_elastic_client("local")

    try:
        # Try to retrieve the index mapping
        mapping = client.indices.get_mapping(index=index_name)
        # print(f"The index '{index_name}' exists.")
    except NotFoundError:
        # print(f"The index '{index_name}' does not exist.")
        return False
    
    # Define the query to search for the fdc_id
    query = {
        "query": {
            "term": {
                "fdc_id": fdc_id
            }
        }
    }

    # Search for the fdc_id in the index
    response = client.search(index=index_name, body=query)

    # Check if any documents match the query
    if response["hits"]["total"]["value"] > 0:
        return True
    else:
        return False

def calculate_nutrients_score(food_item):
    # simple ranking of foods based on % of daily values - hight protein, dietary fiber is good - 
    # suger, trans fat, high sodium and carbohydrates is bad, 
    # extra points for the superfoods category
    #  
    score = 0
    
    description = food_item['description']
    fdcid = food_item['fdc_id']

    # if item is in index - no need to call api again
    if not check_if_id_is_in_es_index("nutrients", fdcid):
        food_info = get_food_info(fdcid)
        if food_info:
            if food_info.get("labelNutrients"):
                # {'fat': {'value': 0.3}, 'saturatedFat': {'value': 0.0}, 'transFat': {'value': 0.0}, 
                # 'cholesterol': {'value': 0.0}, 'sodium': {'value': 43.0}, 'carbohydrates': {'value': 4.2}, 
                # 'fiber': {'value': 34.6}, 'sugars': {'value': 4.2}, 'protein': {'value': 4.3}, 'calcium': {'value': 0.0}, 
                # 'iron': {'value': 0.0}, 'potassium': {'value': 0.0}, 'calories': {'value': 37.0}}
                label_nutrients = food_info.get("labelNutrients")
                label_data = fix_units(label_nutrients)
            else:
                #print(food_info.keys())
                #print(food_info.get("foodNutrients"))
                label_data = fix_nutrition_labels(food_item["food_nutrients"])
            # we will have labelNutrients or food_nutrients under labelNutrients for all as food_nutrients are not properly pro-rated in some case
            food_item["labelNutrients"] = label_data
        else:
            #print(food_info)
            label_data = {}
    else:
        label_data = {}
    
    nutrition_data = fix_nutrition_labels(food_item["food_nutrients"])
    food_item["food_nutrients"] = nutrition_data
    if not label_data: label_data = nutrition_data

    # Positive scores for higher protein and fiber content
    if label_data.get('protein'):
        if label_data['protein'].get('unit_name') == 'g':
            score += label_data['protein']['amount'] / daily_values['protein']['gram'] * 0.2
        else:
            # convert iu to grams
            print(f"{food_item['fid']} - protein - units: {label_data['protein'].get('unit_name')}")

    if label_data.get('fiber'):
        if label_data['fiber'].get('unit_name') == 'g':
            score += label_data['fiber']['amount'] / daily_values['fiber']['gram'] * 0.2
        else:
            print(f"{food_item['fid']} - fiber - units: {label_data['fiber'].get('unit_name')}")

    # Extra points for superfoods
    # Tokenize the description
    tokens = nltk.word_tokenize(description)

    for token in tokens:
        if token.lower() in superfoods:
            score += 0.2
    
    # Negative scores for higher saturated fat, cholesterol, sodium, total carbs, and total sugars
    if label_data.get('alcohol'):
        if label_data['alcohol'].get('unit_name') == 'g':
            score -= label_data['alcohol']['amount'] / daily_values['alcohol']['gram'] * 0.35
        else:
            print(f"{food_item['fid']} - alcohol - units: {label_data['alcohol'].get('unit_name')}")

    if label_data.get('fat'):
        if label_data['fat'].get('unit_name') == 'g':
            score -= label_data['fat']['amount'] / daily_values['fat']['gram'] * 0.15
        else:
            print(f"{food_item['fid']} - lipid - units: {label_data['fat'].get('unit_name')}")

    if label_data.get('transFat'):
        if label_data['transFat'].get('unit_name') == 'g':
            score -= label_data['transFat']['amount'] / daily_values['fat']['gram'] * 0.1
        else:
            print(f"{food_item['fid']} - trans fat - units: {label_data['transFat'].get('unit_name')}")

    if label_data.get('saturatedFat'): 
        if label_data['saturatedFat'].get('unit_name') == 'g':
            score -= label_data['saturatedFat']['amount'] / daily_values['saturatedFat']['gram'] * 0.1
        else:
            print(f"{food_item['fid']} - saturated - units: {label_data['saturatedFat'].get('unit_name')}")

    if label_data.get('manounsaturatedFat'):
        if label_data['manounsaturatedFat'].get('unit_name') == 'g':
            score -= label_data['manounsaturatedFat']['amount'] / daily_values['fat']['gram'] * 0.05
        else:
            print(f"{food_item['fid']} - monosaturated - units: {label_data['manounsaturatedFat'].get('unit_name')}")

    if label_data.get('polyunsaturatedFat'):
        if label_data['polyunsaturatedFat'].get('unit_name') == 'g':
            score -= label_data['polyunsaturatedFat']['amount'] / daily_values['fat']['gram'] * 0.05
        else:
            print(f"{food_item['fid']} - polyunsaturated - units: {label_data['polyunsaturatedFat'].get('unit_name')}")

    if label_data.get('cholesterol'):
        if label_data['cholesterol'].get('unit_name') == 'mg':
            score -= label_data['cholesterol']['amount'] / daily_values['cholesterol']['milligram'] * 0.15
        else:
            print(f"{food_item['fid']} - cholesterol - units: {label_data['cholesterol'].get('unit_name')}")
    
    if label_data.get('sodium'):
        if label_data['sodium'].get('unit_name') == 'mg':
            score -= label_data['sodium']['amount'] / daily_values['sodium']['milligram'] * 0.2
        else:
            print(f"{food_item['fid']} - sodium - units: {label_data['sodium'].get('unit_name')}")
    
    if label_data.get('carbohydrates'):
        if label_data['carbohydrates'].get('unit_name') == 'g':
            score -= label_data['carbohydrates']['amount'] / daily_values['carbohydrates']['gram'] * 0.25
        else:
            print(f"{food_item['fid']} - carbohydrates - units: {label_data['carbohydrates'].get('unit_name')}")
    
    # not all foods have this
    if label_data.get('addedSugar'):
        if label_data['addedSugar'].get('unit_name') == 'g':
            score -= label_data['addedSugar']['amount'] / daily_values['sugars']['gram'] * 0.15
        else:
            print(f"{food_item['fid']} - sugars added - units: {label_data['addedSugar'].get('unit_name')}")
    
    if label_data.get('sugars'):
        if label_data['sugars'].get('unit_name') == 'g':
            score -= label_data['sugars']['amount'] / daily_values['sugars']['gram'] * 0.15
        else:
            print(f"{food_item['fid']} - sugars total - units: {label_data['sugars'].get('unit_name')}")
    
    return score, label_data

def calculate_recipes_score(food_item):
    # simple ranking of foods based on % of daily values - hight protein, dietary fiber is good - 
    # suger, trans fat, high sodium and carbohydrates is bad, 
    # extra points for the superfoods category
    #  
    score = 0
    
    description = food_item['description']
    nutrition_data = food_item["nutrients"]

    # Positive scores for higher protein and fiber content
    if nutrition_data.get('protein'):
        if nutrition_data['protein'].get('unit_name') == 'g':
            score += nutrition_data['protein']['amount'] / daily_values['protein']['gram'] * 0.2
        else:
            # convert iu to grams
            print(f"{food_item['fid']} - protein - units: {nutrition_data['protein'].get('unit_name')}")

    if nutrition_data.get('fiber'):
        if nutrition_data['fiber'].get('unit_name') == 'g':
            score += nutrition_data['fiber']['amount'] / daily_values['fiber']['gram'] * 0.2
        else:
            print(f"{food_item['fid']} - fiber - units: {nutrition_data['fiber'].get('unit_name')}")

    # Extra points for superfoods
    # Tokenize the description
    tokens = nltk.word_tokenize(description)

    for token in tokens:
        if token.lower() in superfoods:
            score += 0.2
    
    # Negative scores for higher saturated fat, cholesterol, sodium, total carbs, and total sugars
    if nutrition_data.get('alcohol'):
        if nutrition_data['alcohol'].get('unit_name') == 'g':
            score -= nutrition_data['alcohol']['amount'] / daily_values['alcohol']['gram'] * 0.35
        else:
            print(f"{food_item['fid']} - alcohol - units: {nutrition_data['alcohol'].get('unit_name')}")

    if nutrition_data.get('fat'):
        if nutrition_data['fat'].get('unit_name') == 'g':
            score -= nutrition_data['fat']['amount'] / daily_values['fat']['gram'] * 0.15
        else:
            print(f"{food_item['fid']} - lipid - units: {nutrition_data['fat'].get('unit_name')}")

    if nutrition_data.get('transFat'):
        if nutrition_data['transFat'].get('unit_name') == 'g':
            score -= nutrition_data['transFat']['amount'] / daily_values['fat']['gram'] * 0.1
        else:
            print(f"{food_item['fid']} - trans fat - units: {nutrition_data['transFat'].get('unit_name')}")

    if nutrition_data.get('saturatedFat'): 
        if nutrition_data['saturatedFat'].get('unit_name') == 'g':
            score -= nutrition_data['saturatedFat']['amount'] / daily_values['saturatedFat']['gram'] * 0.1
        else:
            print(f"{food_item['fid']} - saturated - units: {nutrition_data['saturatedFat'].get('unit_name')}")

    if nutrition_data.get('manounsaturatedFat'):
        if nutrition_data['manounsaturatedFat'].get('unit_name') == 'g':
            score -= nutrition_data['manounsaturatedFat']['amount'] / daily_values['fat']['gram'] * 0.05
        else:
            print(f"{food_item['fid']} - monosaturated - units: {nutrition_data['manounsaturatedFat'].get('unit_name')}")

    if nutrition_data.get('polyunsaturatedFat'):
        if nutrition_data['polyunsaturatedFat'].get('unit_name') == 'g':
            score -= nutrition_data['polyunsaturatedFat']['amount'] / daily_values['fat']['gram'] * 0.05
        else:
            print(f"{food_item['fid']} - polyunsaturated - units: {nutrition_data['polyunsaturatedFat'].get('unit_name')}")

    if nutrition_data.get('cholesterol'):
        if nutrition_data['cholesterol'].get('unit_name') == 'mg':
            score -= nutrition_data['cholesterol']['amount'] / daily_values['cholesterol']['milligram'] * 0.15
        else:
            print(f"{food_item['fid']} - cholesterol - units: {nutrition_data['cholesterol'].get('unit_name')}")
    
    if nutrition_data.get('sodium'):
        if nutrition_data['sodium'].get('unit_name') == 'mg':
            score -= nutrition_data['sodium']['amount'] / daily_values['sodium']['milligram'] * 0.2
        else:
            print(f"{food_item['fid']} - sodium - units: {nutrition_data['sodium'].get('unit_name')}")
    
    if nutrition_data.get('carbohydrates'):
        if nutrition_data['carbohydrates'].get('unit_name') == 'g':
            score -= nutrition_data['carbohydrates']['amount'] / daily_values['carbohydrates']['gram'] * 0.25
        else:
            print(f"{food_item['fid']} - carbohydrates - units: {nutrition_data['carbohydrates'].get('unit_name')}")
    
    # not all foods have this
    if nutrition_data.get('addedSugar'):
        if nutrition_data['addedSugar'].get('unit_name') == 'g':
            score -= nutrition_data['addedSugar']['amount'] / daily_values['sugars']['gram'] * 0.15
        else:
            print(f"{food_item['fid']} - sugars added - units: {nutrition_data['addedSugar'].get('unit_name')}")
    
    if nutrition_data.get('sugars'):
        if nutrition_data['sugars'].get('unit_name') == 'g':
            score -= nutrition_data['sugars']['amount'] / daily_values['sugars']['gram'] * 0.15
        else:
            print(f"{food_item['fid']} - sugars total - units: {nutrition_data['sugars'].get('unit_name')}")
    
    return score

# distinct food_nutrients sub fields in usda dataset:
# Fatty acids, total saturated
# Vitamin D (D2 + D3), International Units
# Carbohydrate, by difference
# Protein
# Cholesterol
# Fiber, total dietary
# Potassium, K
# Vitamin A, IU
# Vitamin C, total ascorbic acid
# Sugars, total including NLEA
# Iron, Fe
# Sugars, added
# Calcium, Ca
# Total lipid (fat)
# Sodium, Na
# Energy
# Fatty acids, total trans