import os, json, openai

def ask_openai_davinci(question: str, api_key: str):
    """ expands generic question into a collection of searchable queries """
    openai.api_key = api_key
    prompt = (f"{question}\n"
              f"Answer:")
   
    # engine="text-davinci-003",
    # model="gpt-3.5-turbo",
    response = openai.Completion.create(
        engine="text-davinci-003",
        prompt=prompt,
        max_tokens=2048,
        n=1,
        stop=None,
        temperature=0.6,
    )

    answer = response.choices[0].text.strip()
    return answer


def ask_openai_turbo(question: str, api_key: str):
    """ expands generic question into a collection of searchable queries """
    openai.api_key = api_key
    prompt = (f"{question}\n"
              f"Answer:")

    # https://cobusgreyling.medium.com/example-code-implementation-considerations-for-gpt-3-5-turbo-chatml-whisper-e61f8703c5db
    # response = openai.Completion.create(
    #    engine="text-davinci-003",
    #    prompt=prompt,
    #    max_tokens=1024,
    #    n=1,
    #    stop=None,
    #    temperature=0.6,
    #)
    # answer = response.choices[0].text.strip()

    response = openai.ChatCompletion.create( # Change this
        model = "gpt-3.5-turbo", # Change this
        messages = [ # Change this
            {"role": "assistant", "content": prompt}
        ],
        max_tokens = 2048,
        n = 1,
        temperature = 0.6,
    )
    
    answer = response['choices'][0]['message']['content']
    return answer


def json_to_dict(json_text, openai_key):
    try:
        data_dict = json.loads(json_text)
        return data_dict
    except ValueError as e:
        print("Failed to json load prompt reply, retrying:", e)
        prompt = (
            f"Please correct json formatting issues: {e}"
            f"document: {json_text}"
        )
        answer = ask_openai_davinci(prompt, openai_key)
        try:
            data_dict = json.loads(answer)
            return data_dict
        except ValueError as e:
            print(e)
            return json_text

        
def flatten_json_doc(doc, openai_key):
    prompt = (
        "Flatten the document to output only the following fields in properly delimited json format - double-quoted property names and text values, escape special characters."
        "Remove order numbers from the ingredients list. Update cook_time and total_time if available from other soources."
        "Convert tips into a new line separated double-quoted text: {\"tips\": text}." 
        "List the ingredients and their quantities as new-line separated double-quoted text: {\"ingredients\": text}."
        "If ingredient quantity is missing, add estimated quantity in natural units from similar recipes."
        "Use field 'estimated' to list all estimated quantities as new line separated double-quoted text: {\"estimated\": text}\n"
        "Fields to keep:\n"
        "name: str\n"
        "link: str\n"
        "photo: str\n"
        "description: str\n"
        "ingredients: str\n"
        "nutrients: str\n"
        "cook_time: str\n"
        "total_time: str\n"
        "directions: str\n"
        "tips: str\n"
        "source: str\n"
        "prep_time: str\n"
        "calories: str\n"
        "servings: str\n"
        "video: str\n"
        "serving_size: str\n"
        "Only keep values of nutrients in the following list and convert to proper units per serving size:\n"
        "calories in kcal\n"
        "total_fat in grams\n"
        "saturated_fat in grams\n"
        "trans_fat in grams\n"
        "polyunsaturated_fat in grams\n"
        "monounsaturated_fat in grams\n"
        "cholesterol in milligrams\n"
        "sodium in milligrams\n"
        "total_carbs in grams\n"
        "dietary_fiber in grams\n"
        "total_sugars in grams\n"
        "added_sugars in grams\n"
        "protein in grams\n"
        "vitamin D in microgram\n"
        "calcium in milligram\n"
        "iron in milligram\n"
        "potassium in milligram\n"
        "vitamin A in microgram\n"
        "vitamin C in microgram\n"
        f"document: {doc}"
    )
    #answer = ask_openai_turbo(prompt)
    answer = ask_openai_davinci(prompt, openai_key)
    return json_to_dict(answer, openai_key)


def compare_recipes_health_with_openai(recipe1, recipe2, openai_key):
    """ function to rank recipes - with sufficient number of pairs compared we can rank all recipes and fit an xgboost model to apply predictions to new recipes"""
    # VERY EXPENSIVE TO RUN - 800usd+ for a reasonable size dataset
    prompt = (
        "Compare the following two recipes and determine which one is healthier. "
        "Consider parameters like nutritional values, superfoods, calories, fat, sugar, and carbohydrates content. "
        "Please provide the result in the following structured JSON format:\n"
        "{\n"
        "  \"healthier_recipe\": [recipe number],\n"
        "  \"reasons\": [reasons for the selection],\n"
        "  \"parameters_used\": [parameters considered]\n"
        "}\n"
        "Recipe 1: " + json.dumps(recipe1) +
        "\nRecipe 2: " + json.dumps(recipe2)
    )

    answer = ask_openai_davinci(prompt, openai_key)

    # Assuming answer is a valid JSON string
    result = json.loads(answer)

    healthier_recipe = result["healthier_recipe"]
    reasons = result["reasons"]
    parameters_used = result["parameters_used"]

    return healthier_recipe, reasons, parameters_used


def jsonfile_to_doc(file_paths):
    all_docs = []
    OPENAI_API_KEY=os.environ.get("OPENAI_API_KEY")

    for file_path in file_paths:
        output_file_path = file_path.rsplit(".", 1)[0].replace("_processed","_davinci") + ".json"
        with open(file_path, "r") as input_file, open(output_file_path, "w") as output_file:
            for line in input_file:
                line = line.strip()
                if line:
                    doc = flatten_json_doc(line, OPENAI_API_KEY)
                    all_docs.append(doc)
                    try:
                        output_file.write(json.dumps(doc))
                    except:
                        output_file.write(doc)
    return all_docs, output_file_path


def flatten_dicts_to_text(dictionary):
    text = ""
    for field, val in dictionary.items():
        if isinstance(val, dict):
            subtext = flatten_dicts_to_text(val)
            text += f"{field}: {subtext}"
        else:
            text += f"{field}: {val}/n"
    return text


def parse_text_to_dict(text):
    lines = text.split('\n')
    result = {}
    
    for line in lines:
        if line.strip() != '':
            parts = line.split(':', 1)
            field = parts[0].strip()
            rest = parts[1].strip()
            
            if ':' in rest:
                sub_dict = parse_text_to_dict(rest)
                result[field] = sub_dict
            else:
                units, val = rest.split(':', 1)
                result[field] = {
                    'units': units.strip(),
                    'value': val.strip()
                }
    
    return result
