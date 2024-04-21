import difflib
import json
import pandas as pd
from itertools import chain
import multiprocessing as mp

def parse_ref(ref):
  masechet = ref.split(" ")[:-1]
  masechet= " ".join(masechet)
  loc1= ref.split(" ")[-1].split("-")[0]
  if(len(ref.split(" ")[1].split("-"))==1):
    return masechet,loc1,None
  else:
    loc2= ref.split(" ")[1].split("-")[1]
    return masechet,loc1,loc2
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
def get_talmud_text(ref):
    masechet,loc1,loc2= parse_ref(ref)
    text= ""
    with open("/Users/ephraimmeiri/gitEtc/Rashi Thesis work/talmud_files/"+masechet.replace(" ","%20")+".json") as f:
        data = json.load(f)
        index1,line1 = None,None # I'm doing it this way because of the multi-daf behavior
        if ":" not in loc1: # no line
            index1= daf_to_index(loc1)
        else: # has line
            index1= daf_to_index(loc1.split(":")[0])
            line1= int(loc1.split(":")[1])-1
        if loc2==None: # Since loc may have a line range (or just a single line)
            if ":" not in loc1: # no line
                text= data['text'][index1]
            else: # has line
                text= data['text'][index1][line1]
        elif not any(s in loc2 for s in ['a','b']): # loc 2 is a line within this daf
                text= data['text'][index1][line1:int(loc2)]
        else:
            if ":" not in loc2: # no line
                index2= daf_to_index(loc2)
                text= data['text'][index1:index2]
            else: # has
                index2= daf_to_index(loc2.split(":")[0])
                line2= int(loc2.split(":")[1])-1
                text = []
                for i in range(index1,index2+1):
                    # print(i)
                    if(i==index1):
                        text.append(data['text'][i][line1:][0])
                    elif(i==index2):
                        text.append(data['text'][i][:line2+1][0])
                    else:
                        text.append(data['text'][i][0])
    while isinstance(text, list):
        # Join the list into a single string
        text = ' '.join(text)
    return text

# def get_text(ref):
#   base_URL= "http://www.sefaria.org/api/texts/"
#   URL= base_URL + ref
#   req = requests.get(url = URL).json()
#   txt= req['he']
#   depth = lambda L: isinstance(L, list) and max(map(depth, L))+1
#   if(depth(txt)>1):
#     txt = list(chain.from_iterable(req['he']))
#   return txt
def get_stats(ref1,ref2):
  txt1= get_talmud_text(ref1)
  txt2= get_talmud_text(ref2)
  length = min(len(txt1),len(txt2))
  similarity = difflib.SequenceMatcher(None, txt1, txt2).ratio()
  return length,similarity
def get_all_stats(mesorat_hashas):
  df = pd.DataFrame(columns=["ref1","ref2","length","similarity"])
  i=0
  for r in mesorat_hashas:
    i+=1
    if i%1000==0:
        print(i)
    elif i%100==0:
        print(i,end=". ")
    # print(i,end=". ")
    stats= get_stats(r[0], r[1])
    new_row= pd.Series([r[0], r[1],stats[0],stats[1]])
    df = pd.concat([df,new_row], ignore_index=True)
  return df


def get_stats_wrapper(args):
    """
    Wrapper function to handle arguments for get_stats
    """
    ref1, ref2 = args
    return ref1, ref2, *get_stats(ref1, ref2)


def get_all_stats_parallel(mesorat_hashas):
    pool = mp.Pool()
    results = pool.map(get_stats_wrapper, mesorat_hashas)
    pool.close()
    pool.join()

    df = pd.DataFrame(results, columns=["ref1", "ref2", "length", "similarity"])
    return df

if __name__ == '__main__':
    mesorat_hashas_path2 = "/Users/ephraimmeiri/Downloads/mesorat_hashas_links.json"
    # mesorat_hashas_df = pd.read_json(mesorat_hashas_file2)
    mesorat_hashas_file = open(mesorat_hashas_path2)
    mesorat_hashas_json = json.load(mesorat_hashas_file)
    # mesorat_hashas= pd.DataFrame(r.json())
    masechtot = ["Kodashim/Arakhin","Kodashim/Bekhorot", "Kodashim/Chullin","Kodashim/Keritot","Kodashim/Meilah","Kodashim/Menachot","Kodashim/Tamid","Kodashim/Temurah","Kodashim/Zevachim",
                 "Moed/Beitzah","Moed/Chagigah","Moed/Eruvin","Moed/Pesachim","Moed/Megillah","Moed/Moed%20Katan","Moed/Rosh%20Hashanah","Moed/Shabbat","Moed/Sukkah","Moed/Taanit","Moed/Yoma",
                 "Nashim/Gittin","Nashim/Ketubot","Nashim/Kiddushin","Nashim/Nedarim","Nashim/Nazir","Nashim/Sotah","Nashim/Yevamot",
                 "Nezikin/Bava%20Batra","Nezikin/Bava%20Kamma","Nezikin/Bava%20Metzia","Nezikin/Avodah%20Zarah","Nezikin/Horayot","Nezikin/Makkot","Nezikin/Sanhedrin","Nezikin/Shevuot",
                 "Tahorot/Niddah", "Zeraim/Berakhot"]
    masechtot_names= [s.split("/")[1].split("%20")[0] for s in masechtot]
    print(masechtot_names)
    mesorat_hashas_filtered = [link for link in mesorat_hashas_json
                               if link['refs'][0].split(" ")[0] in masechtot_names
                               and link['refs'][1].split(" ")[0] in masechtot_names]
    shas_refs= [(link['refs'][0],link['refs'][1]) for link in mesorat_hashas_filtered]
    # print(get_stats(shas_refs[0][0],shas_refs[0][1]))
    # txt1= get_talmud_text(shas_refs[0][0])
    # print(txt1)
    # print(type(txt1),len(txt1))
    # txt2= get_talmud_text(shas_refs[0][1])
    # print(txt2)
    # print(type(txt2),len(txt2))
    print(len(shas_refs))
    # for r in shas_refs:
    #     print(r)
    # get_talmud_text('Yoma 55b:6-56b:1')

    # print(get_talmud_text("Chullin 100b:1-2"))
    # print(get_talmud_text("Menachot 23a:1"))
    df = get_all_stats_parallel(shas_refs)
    with open("all_shas_stats.csv", "w") as f:
        df.to_csv(f)