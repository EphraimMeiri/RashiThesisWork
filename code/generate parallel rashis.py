import os
from multiprocessing import Pool

import numpy as np
import pandas as pd
import re
import difflib
import json
import math

rashi_masechtot = ['Shabbat','Eruvin','Pesachim',
    'Arakhin', 'Bekhorot', 'Chullin', 'Keritot', 'Menachot', 'Temurah', 'Zevachim', 'Beitzah',
    'Chagigah',   'Megillah', 'Rosh Hashanah',  'Sukkah', 'Yoma', 'Gittin', 'Ketubot', 'Kiddushin', 'Sotah', 'Yevamot', 'Bava Batra', 'Bava Kamma', 'Bava Metzia', 'Avodah Zarah', 'Makkot', 'Sanhedrin', 'Shevuot', 'Niddah', 'Berakhot']

mesorat_hashas_path2 = "/Users/ephraimmeiri/gitEtc/Rashi Thesis work/mesorat_hashas_links.json"
mesorat_hashas_file = open(mesorat_hashas_path2)
mesorat_hashas_json = json.load(mesorat_hashas_file)
mesorat_hashas_filtered = [link for link in mesorat_hashas_json
                           if  " ".join(link['refs'][0].split(" ")[:-1]) in rashi_masechtot
                           and " ".join(link['refs'][1].split(" ")[:-1]) in rashi_masechtot
                           ]
# print(len(hashlamot_links))
shas_refs = [(link['refs'][0],link['refs'][1]) for link in mesorat_hashas_filtered]


def get_rashi_file(masechet):
    file = "/Users/ephraimmeiri/gitEtc/Rashi Thesis work/Rashi plaintext/json filtered/Rashi on "+masechet+".json"
    if os.path.exists(file):
        with open("/Users/ephraimmeiri/gitEtc/Rashi Thesis work//Rashi plaintext/json filtered/Rashi on "+masechet+".json") as f:
            data = json.load(f)
            return data
    else:
        return None

    # Possible cases for all locations:
    # 1. loc1 is a single daf, eg "2a"
    # 2. loc1 is a single daf with a line, eg "2a:3"
    # 3. loc1 i a singe daf with a range of lines, eg "2a:3-4"
    # 4. loc1 is a range of dafs, eg "2a-3b"
    # 5. loc1 is a range of dafs with a line, eg "2a:3-3b:4"
    # I think we might only really need to care about cases 2,3, and 5 for our links
# def get_rashi_ref(ref,join=False):
#     masechet,loc1,loc2= parse_ref(ref)
#     rashi_file= get_rashi_file(masechet)
#     if rashi_file is None:
#         return None
#     text= ""
#     with open("/Users/ephraimmeiri/gitEtc/Rashi Thesis work/Rashi plaintext/json undisputed/Rashi on "+masechet+".json") as f:
#         data = json.load(f)['text']
#         if loc2 is None:
#             if "-" not in loc1:
#                 daf, line = loc1.split(":")
#                 # print(line)
#                 daf_loc = daf_to_index(daf)
#                 if int(line) <= len(data[daf_loc]):
#                     return data[daf_loc][int(line) - 1]  # TODO: Should this be line-1?
#                 else:
#                     return None  # No rashi on this line
#             elif "-" in loc1:
#                 daf, lines = loc1.split(":")
#                 line1, line2 = lines.split("-")
#                 daf_loc = daf_to_index(daf)
#                 if int(line1) <= len(data[daf_loc]):
#                     return data[daf_loc][int(line1) :int(line2)]  # TODO: Should this be line-1?
#                 else:
#                     return None  # No rashi on this line
#         else:
#             daf1, line1 = loc1.split(":")
#             daf2, line2 = loc2.split(":")
#             daf_loc1 = daf_to_index(daf1)
#             daf_loc2 = daf_to_index(daf2)
#             text = []
#             if int(line1) <= len(data[daf_loc1]):
#                 text.append(data[daf_loc1][int(line1) - 1:])
#             for daf in range(daf_loc1+1, daf_loc2):
#                 text.append(data[daf])
#             if int(line2) <= len(data[daf_loc2]):
#                 text.append(data[daf_loc2][:int(line2)])
#         if join:
#             while isinstance(text, list):
#                 # Join the list into a single string
#                 text = ' '.join(text)
#         return text

def parse_ref(ref):
  masechet,dappim = ref.rsplit(" ",1) # Get all but the last element
  # masechet= " ".join(masechet)
  colons = dappim.count(":")
  if colons == 0:
    return masechet,dappim,None
  elif colons == 2:
    loc1,loc2= dappim.split("-")
    return masechet,loc1,loc2
  else:
    return masechet,dappim,None

  # if(len(ref.split(" ")[1].split("-"))==1):
  #   return masechet,loc1,None
  # else:
  #   loc2= ref.split(" ")[1].split("-")[1]
  #   return masechet,loc1,loc2
def daf_to_index(ref):
    daf = int(ref[:-1])
    amud= ref[-1]
    index= ((daf-1)*2)
    if(amud=="b"):
      index+= 1
    return index
def get_talmud_file(masechet):
    masechet = masechet.replace(" ","%20")
    with open("/Users/ephraimmeiri/gitEtc/Rashi Thesis work/talmud_files/"+masechet+".json") as f:
        data = json.load(f)
        return data

# def get_talmud_text(ref):
#     masechet,loc1,loc2= parse_ref(ref)
#     text= ""
#     with open("/Users/ephraimmeiri/gitEtc/Rashi Thesis work/talmud_files/"+masechet.replace(" ","%20")+".json") as f:
#         data = json.load(f)
#         index1,line1 = None,None # I'm doing it this way because of the multi-daf behavior
#         if ":" not in loc1: # no line
#             index1= daf_to_index(loc1)
#         else: # has line
#             index1= daf_to_index(loc1.split(":")[0])
#             line1= int(loc1.split(":")[1])-1
#         if loc2==None: # Since loc may have a line range (or just a single line)
#             if ":" not in loc1: # no line
#                 text= data['text'][index1]
#             else: # has line
#                 text= data['text'][index1][line1]
#         elif not any(s in loc2 for s in ['a','b']): # loc 2 is a line within this daf
#                 text= data['text'][index1][line1:int(loc2)]
#         else:
#             if ":" not in loc2: # no line
#                 index2= daf_to_index(loc2)
#                 text= data['text'][index1:index2]
#             else: # has
#                 index2= daf_to_index(loc2.split(":")[0])
#                 line2= int(loc2.split(":")[1])-1
#                 text = []
#                 for i in range(index1,index2+1):
#                     # print(i)
#                     if(i==index1):
#                         text.append(data['text'][i][line1:][0])
#                     elif(i==index2):
#                         text.append(data['text'][i][:line2+1][0])
#                     else:
#                         text.append(data['text'][i][0])
#     while isinstance(text, list):
#         # Join the list into a single string
#         text = ' '.join(text)
#     return text

def get_talmud_text(ref,join):
    masechet, loc1, loc2 = parse_ref(ref)
    file = get_talmud_file(masechet)
    return get_text(file, loc1, loc2, join)

def get_rashi_text(ref,join=False):
    masechet, loc1, loc2 = parse_ref(ref)
    file = get_rashi_file(masechet)
    return get_text(file, loc1, loc2, join)

def get_text(file,loc1,loc2,join=False):
    text= ""
    data = file['text']
    if loc2 is None:
        if "-" not in loc1 and ":" in loc1:
            daf, line = loc1.split(":")
            # print(line)
            daf_loc = daf_to_index(daf)
            if daf_loc<len(data) and int(line) <= len(data[daf_loc]):
                text = data[daf_loc][int(line) - 1]  # TODO: Should this be line-1?
            else:
                return None  # No rashi on this line
        elif "-" in loc1 and ":" in loc1 :
            daf, lines = loc1.split(":")
            line1, line2 = lines.split("-")
            daf_loc = daf_to_index(daf)
            if daf_loc<len(data) and int(line1) <= len(data[daf_loc]):
                text = data[daf_loc][int(line1)-1 :int(line2)]
            else:
                return None  # No rashi on this line
    else:
        daf1, line1 = loc1.split(":")
        daf2, line2 = loc2.split(":")
        daf_loc1 = daf_to_index(daf1)
        daf_loc2 = daf_to_index(daf2)
        text = []
        if daf_loc1<len(data) and int(line1) <= len(data[daf_loc1]):
            text.append(data[daf_loc1][int(line1) - 1:])
        for daf in range(daf_loc1+1, daf_loc2):
            text.append(data[daf])
        if daf_loc2 < len(data) :
            if int(line2) <= len(data[daf_loc2]):
                text.append(data[daf_loc2][:int(line2)])
            else:
                text.append(data[daf_loc2])

    if join and isinstance(text, list):
        joined = ""
        for t in text:
            joined += " ".join(t)
        text= joined
    return text

def get_rashis(link):
    ref1,ref2 = link
    rashi_text1 = get_rashi_text(ref1)
    rashi_text2 = get_rashi_text(ref2)
    return rashi_text1, rashi_text2

def get_match(matcher):
    a_start, b_start, = matcher.find_longest_match().a, matcher.find_longest_match().b
    a_end, b_end = a_start , b_start
    for match in matcher.get_matching_blocks()[:-1]: # "The last triple is a dummy, and has the value (len(a), len(b), 0)."
        if match.a < a_start:
            a_start= match.a
        if match.b < b_start:
            b_start= match.b
        if match.a + match.size > a_end:
            a_end= match.a + match.size
        if match.b + match.size > b_end:
            b_end= match.b + match.size
    return a_start, a_end, b_start, b_end

def sv_in_tal(rashi,tal,threshold=0.4):
    sv= rashi.split(" - ")[0]
    matcher = difflib.SequenceMatcher(None, sv, tal)
    match= matcher.find_longest_match()
    match = get_match(matcher)
    tal_extract = tal[match[2]:match[3]]
    matcher2= difflib.SequenceMatcher(None, sv, tal_extract)
    return matcher2.ratio()>threshold
    # return tal[match[2]:match[3]]

def cross_ref_sv(rashi_text,tal):
    selected_rashis = []
    for rashi in rashi_text:
        if type(rashi)==str and sv_in_tal(rashi,tal):
            selected_rashis.append(rashi)
        elif type(rashi)==list:
            if not rashi: # If the list is empty
                continue
            if isinstance(rashi[0],str):
                for r in rashi:
                    if sv_in_tal(r,tal):
                        selected_rashis.append(r)
            else:
                for line in rashi:
                    for r in line:
                        if sv_in_tal(r,tal):
                            selected_rashis.append(r)
    return selected_rashis
def get_rashis_conservative(link):
    ref1,ref2 = link
    rashi_text1 = get_rashi_text(ref1)
    rashi_text2 = get_rashi_text(ref2)
    if rashi_text1 is None or rashi_text2 is None:
        return None, None
    tal_text1 = get_talmud_text(ref1,join=True)
    tal_text2 = get_talmud_text(ref2,join=True)
    rashis1 = cross_ref_sv(rashi_text1,tal_text2)
    rashis2 = cross_ref_sv(rashi_text2,tal_text1)
    return rashis1, rashis2

def get_ratios(file_path,write_file=False):
    paralles = pd.read_csv(file_path,header=0,index_col=0)
    ratios = []
    processed_lines = 0
    for mas1 in rashi_masechtot:
        for mas2 in rashi_masechtot:
            if mas1==mas2:
                continue
            paralles_mas = paralles[(paralles["masechet1"]==mas1) & (paralles["masechet2"]==mas2)]
            processed_lines += len(paralles_mas)
            sum1 = sum([len(r.split(" ")) for r in paralles_mas["txt1"]])
            sum2 = sum([len(r.split(" ")) for r in paralles_mas["txt2"]])
            paralles_mas = paralles[(paralles["masechet1"]==mas2) & (paralles["masechet2"]==mas1)]
            processed_lines += len(paralles_mas)
            sum1 += sum([len(r.split(" ")) for r in paralles_mas["txt2"]])
            sum2 += sum([len(r.split(" ")) for r in paralles_mas["txt1"]])
            if sum1==0 or sum2==0:
                # print(f"Sum is 0 for {mas1} and {mas2}")
                continue
            else:
                ratios.append((mas1,mas2,sum1/sum2))
    print(f"Ratios found for {len(ratios)} masechtot pairs of {len(rashi_masechtot)*len(rashi_masechtot) - len(rashi_masechtot)}")
    print(f"Processed {processed_lines} lines of {len(paralles)}")
    # convert mas1 and mas2 to be col/row
    ratio_df = pd.DataFrame(columns=rashi_masechtot,index=rashi_masechtot)
    for r in ratios:
        ratio_df.at[r[0],r[1]] = r[2]
    # make sure inverse ratios are filled in with the inverse
    for mas1 in rashi_masechtot:
        for mas2 in rashi_masechtot:
            if mas1==mas2:
                continue
            if pd.isna(ratio_df.at[mas1, mas2]) and pd.isna(ratio_df.at[mas2, mas1]):
                continue
            elif pd.isna(ratio_df.at[mas1, mas2]) or pd.isna(ratio_df.at[mas2, mas1]):
                print(f"Error: {mas1} and {mas2} are not both filled in!")
            # elif not ratio_df.at[mas1,mas2] == 1/ratio_df.at[mas2,mas1]:
            elif not np.isclose(ratio_df.at[mas1, mas2] * ratio_df.at[mas2, mas1], 1, rtol=1e-5):
                print(f"Error: {mas1} and {mas2} are not inverses {ratio_df.at[mas1, mas2]}, {ratio_df.at[mas2, mas1]} ! {1/ratio_df.at[mas1, mas2]}")
    if write_file:
        ratio_df.to_csv("/Users/ephraimmeiri/gitEtc/Rashi Thesis work/parallels/length_ratios.csv")
    return ratio_df

from tqdm import tqdm

def main():
    rashi_output = pd.DataFrame(columns=["masechet1", "masechet2", "loc1", "loc2", "txt1", "txt2"])
    for link in tqdm(shas_refs):
        rashis = get_rashis_conservative(link)
        if rashis[0] is None or rashis[1] is None:
            continue
        masechet1, masechet2, loc1, loc2, rashis0, rashis1= process_link(link)
        new_row = pd.Series([masechet1, masechet2, loc1, loc2, rashis[0], rashis[1]])
        rashi_output = pd.concat([rashi_output, new_row], ignore_index=True)
    with open("/rashi_conservative_output.csv",
              "w") as f:
        rashi_output.to_csv(f)
        print("Done")

def process_link(link):
    rashis = get_rashis_conservative(link)
    if rashis[0] is None or rashis[1] is None:
        return None
    masechet1, loc1 = " ".join(link[0].split(" ")[:-1]), link[0].split(" ")[-1]
    masechet2, loc2 = " ".join(link[1].split(" ")[:-1]), link[1].split(" ")[-1]
    return [masechet1, masechet2, loc1, loc2, rashis[0], rashis[1]]

def main_par():
    with Pool() as p:
        results = list(tqdm(p.imap(process_link, shas_refs), total=len(shas_refs)))

    # Filter out None results and convert to DataFrame
    rashi_output = pd.DataFrame([r for r in results if r is not None and r[4] and r[5]],
                                columns=["masechet1", "masechet2", "loc1", "loc2", "txt1", "txt2"])

    with open("/rashi_conservative_output3.csv", "w") as f:
        rashi_output.to_csv(f)
        print("Done")

if __name__ == "__main__":
    get_ratios("/Users/ephraimmeiri/gitEtc/Rashi Thesis work/parallels/rashi_conservative_output3.csv",write_file=True)
