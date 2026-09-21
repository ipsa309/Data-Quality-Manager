from collections import defaultdict

# Your input list and dictionary
list1 = [
    {'fruit': 'apple', 'vegetable': 'potato', 'class': 'one'},
    {'fruit': 'apple', 'vegetable': 'tomato', 'class': 'one'},
    {'fruit': 'orange', 'vegetable': 'tomato', 'class': 'two'}
]

mail_dict = {
    "mob2": "apple-one",
    "mob2:": "orange-two",
    "mob1": "apple-one"
}

# Initialize defaultdict to group dictionaries by mob
grouped_dict = defaultdict(list)

# Parse through the list of dictionaries
for d in list1:
    # Construct the key using fruit and class
    key = f"{d['fruit']}-{d['class']}"
    
    # Find all groups (mob1, mob2, etc.) in mail_dict that match the key
    for mob_key, value in mail_dict.items():
        if value == key:
            grouped_dict[mob_key].append(d)

# Ensure the desired format: mob1 and mob2 should both include their respective dictionaries
# even if the keys overlap
final_dict = defaultdict(list)
for key in mail_dict.keys():
    final_dict[key] = grouped_dict[key]

# Print the grouped dictionaries
for mob_key, group in final_dict.items():
    print(f'Group "{mob_key}": {group}')
