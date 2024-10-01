import os
import sys

import torch
import json
import pandas as pd
from transformers import BertTokenizer, BertForMaskedLM

# From GPT4
def embed_text(text):
    """
    Convert text into embeddings using Berel model.
    """
    # Tokenize input text
    inputs = tokenizer(text, return_tensors='pt', padding=True, truncation=True, max_length=512)

    # Get embeddings
    with torch.no_grad():
        outputs = model(**inputs)
    embeddings = outputs.last_hidden_state.mean(dim=1).squeeze().numpy()

    return embeddings

def path_to_pd(path):

    # Load the JSON data
    with open(path, 'r', encoding='utf-8') as file:
        rashi_data = json.load(file)

    # Extract the Tractate name (assuming filename without extension for this example)
    tractate_name = rashi_data['title'].split()[-1]  # Modify as needed

    # Extract comments with detailed references (page, line, comment number)
    comments_with_detailed_references = [(comment, daf_num, line_num, comment_num)
                                         for daf_num, daf in enumerate(rashi_data["text"])
                                         for line_num, line in enumerate(daf)
                                         for comment_num, comment in enumerate(line)]

    # Process the data and store in lists
    tractate_data = []
    location_data = []
    verse_data = []
    comment_data = []

    for comment, daf_num, line_num, comment_num in comments_with_detailed_references:
        tractate_data.append(tractate_name)
        location_data.append((daf_num, line_num, comment_num))
        verse_data.append(comment.split("–")[0] if "–" in comment else "")
        comment_data.append(comment)

    # Create a DataFrame
    df = pd.DataFrame({
        'Tractate': tractate_data,
        'Location': location_data,
        'Starting Verse': verse_data,
        'Comment': comment_data
    })

    print(df.head())


def embed_file(path):
    with open('/path/to/your/file.json', 'r', encoding='utf-8') as file:
        data = json.load(file)
    comments_with_detailed_references = [(comment, daf_num, line_num, comment_num)
                                         for daf_num, daf in enumerate(rashi_data["text"])
                                         for line_num, line in enumerate(daf)
                                         for comment_num, comment in enumerate(line)]

    # Convert each comment to embeddings
    embedded_comments = [(embed_text(comment), page_num, line_num) for comment, page_num, line_num in
                         comments_with_references]


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    sys.path.append('./BEREL')
    from rabtokenizer import RabbinicTokenizer
    bert_path= "BEREL"
    tokenizer = RabbinicTokenizer(BertTokenizer.from_pretrained(os.path.join(bert_path, 'vocab.txt')))
    model = BertForMaskedLM.from_pretrained(bert_path)
    path_to_check = 'BEREL'
    file_exists = os.path.exists(os.path.join(path_to_check, 'rabtokenizer.py'))

    print(str(file_exists))
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
