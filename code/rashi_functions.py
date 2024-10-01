from collections import defaultdict

import pandas as pd
import json
import os

rashi_masechtot = ['Arakhin', 'Bekhorot', 'Chullin', 'Keritot', 'Menachot', 'Temurah', 'Zevachim', 'Beitzah', 'Chagigah', 'Eruvin', 'Pesachim', 'Megillah', 'Rosh Hashanah', 'Shabbat', 'Sukkah', 'Yoma', 'Gittin', 'Ketubot', 'Kiddushin', 'Sotah', 'Yevamot', 'Bava Batra', 'Bava Kamma', 'Bava Metzia', 'Avodah Zarah', 'Makkot', 'Sanhedrin', 'Shevuot', 'Niddah', 'Berakhot']
mesorat_hashas_path2 = "/Users/ephraimmeiri/gitEtc/Rashi Thesis work/mesorat_hashas_links.json"
mesorat_hashas_file = open(mesorat_hashas_path2)
mesorat_hashas_json = json.load(mesorat_hashas_file)
mesorat_hashas_filtered = [link for link in mesorat_hashas_json
                           if link['refs'][0].split(" ")[0] in rashi_masechtot
                           and link['refs'][1].split(" ")[0] in rashi_masechtot]
shas_refs = [(link['refs'][0],link['refs'][1]) for link in mesorat_hashas_filtered]

new_name_df= pd.read_csv("/Users/ephraimmeiri/gitEtc/Rashi Thesis work/references subproject/input/PerakimByMaschetBavli.csv", names=["English", "Masechet", "Perakim"], index_col=False)

def find_all(text):
    results = []
    for name in rashi_masechtot:
        file = get_rashi_file(name)
        if file:
            for d in range(len(file['text'])):
                for l in range(len(file['text'][d])):
                    for c in range(len(file['text'][d][l])):
                        if text in file['text'][d][l][c]:
                            results.append([name,d,l,c,file['text'][d][l][c]])
    return pd.DataFrame(results,columns=["Masechet","Daf","Line","Comment","Text"])

def find_all_multi(texts):
    results = []
    for name in rashi_masechtot:
        file = get_rashi_file(name)
        if file:
            for d in range(len(file['text'])):
                for l in range(len(file['text'][d])):
                    for c in range(len(file['text'][d][l])):
                        for text in texts:
                            if text in file['text'][d][l][c]:
                                results.append([name, d, l, c, file['text'][d][l][c]])
    return pd.DataFrame(results, columns=["Masechet", "Daf", "Line", "Comment", "Text"])


def get_rashi_file(masechet):
    file = "/Users/ephraimmeiri/gitEtc/Rashi Thesis work/Rashi plaintext/json filtered/Rashi on "+masechet+".json"
    if os.path.exists(file):
        with open(file) as f:
            data = json.load(f)
            return data
    else:
        return None

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

def is_daf(s):
    return s[-1]=="a" or s[-1]=="b"

def parse_ref2(ref):
    masechet, dappim = ref.rsplit(" ", 1)  # Get all but the last element
    # masechet= " ".join(masechet)
    colons = dappim.count(":")
    if colons == 0:
        return masechet, dappim, None, None,None
    elif colons == 1:
        daf1,b = dappim.split(":")
        if is_daf(b):
            return masechet, daf1, None, b,None
        else:
            if "-" in b:
                line1, line2 = b.split("-")
                return masechet, daf1, line1, daf1,line2
            else:
                return masechet, daf1, b, None,None
    elif colons == 2:
        loc1, loc2 = dappim.split("-")
        daf1, line1 = loc1.split(":")
        daf2, line2 = loc2.split(":")
        return masechet, daf1, line1, daf2, line2
    else:
        print("Error in ref")
        return None

def daf_to_index(ref):
    daf = int(ref[:-1])
    amud= ref[-1]
    index= ((daf-1)*2)
    if(amud=="b"):
      index+= 1
    return index
def index_to_daf(index):
    daf = int(index/2)+1
    amud = "a"
    if index%2==1:
        amud = "b"
    return str(daf)+amud
def get_talmud_file(masechet):
    masechet = masechet.replace(" ","%20")
    with open("/Users/ephraimmeiri/gitEtc/Rashi Thesis work/talmud_files/"+masechet+".json") as f:
        data = json.load(f)
        return data
def get_talmud_text(ref,join):
    masechet, loc1, loc2 = parse_ref(ref)
    file = get_talmud_file(masechet)
    return get_text(file, loc1, loc2, join)
def get_rashi_text(ref,join=False):
    masechet, loc1, loc2 = parse_ref(ref)
    file = get_rashi_file(masechet)
    return get_text(file, loc1, loc2, join)

def ref_in_range(ref1,ref2): # ref2 is the range
    mas1, loc1daf1, loc1line1,loc1daf2, loc1line2 = parse_ref2(ref1)
    mas2, loc2daf1, loc2line1,loc2daf2, loc2line2 = parse_ref2(ref2)
    assert not loc1daf2 # ref1 should be a single location
    loc1daf1 = daf_to_index(loc1daf1)
    loc2daf1 = daf_to_index(loc2daf1)
    loc1line1 = int(loc1line1)
    loc2line1 = int(loc2line1)
    if mas1 != mas2:
        return False
    if not loc2daf2:
        if loc2line2:
            return loc1daf1 == loc2daf1 and loc1line1 >= loc2line1 and loc1line1 <= loc2line2
        else:
            return loc1daf1 == loc2daf1 and loc1line1 == loc2line1
    else:
        loc2daf2 = daf_to_index(loc2daf2)
        loc2line2 = int(loc2line2)
        if loc1daf1 == loc2daf1 and loc1line1 >= loc2line1 and loc1line1 <= loc2line2:
            return True
        elif loc1daf1 == loc2daf2 and loc1line1 >= loc2line1 and loc1line1 <= loc2line2:
            return True
        elif loc1daf1 > loc2daf1 and loc1daf1 < loc2daf2:
            return True
        else:
            return False

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
            if t and isinstance(t,str):
                joined += " "+t
            elif t and isinstance(t,list):
                for tt in t:
                    if tt and isinstance(tt,str):
                        joined += " "+tt
                    elif tt and isinstance(tt,list):
                        joined+= " ".join(tt)
        text= joined
    return text

def get_rashis(link):
    ref1,ref2 = link
    rashi_text1 = get_rashi_text(ref1)
    rashi_text2 = get_rashi_text(ref2)
    return rashi_text1, rashi_text2


def mas_to_heb(name):
    return new_name_df[new_name_df["English"]==name]["Masechet"].values[0].split(",")[0].strip()


def heb_to_mas(name):
    return new_name_df[new_name_df['Masechet'].str.contains(name)]["English"].values[0]

def format_search_results(results):
    output = []
    for i, row in results.iterrows():
        daf = index_to_daf(row['Daf'])
        loc = str(row['Line']) + "," + str(row['Comment'])
        output.append((row['Masechet'], daf, loc, row['Text']))
    return pd.DataFrame(output, columns=["Masechet", "Daf", "Loc", "Text"])
def generate_search(text):
    results_df= find_all(text)
    return format_search_results(results_df)

def generate_multi_search(texts):
    results_df = find_all_multi(texts)
    return format_search_results(results_df)

def get_right_neighbors(query,dist):
    results = []
    for name in rashi_masechtot:
        file = get_rashi_file(name)
        if file:
            for d in range(len(file['text'])):
                for l in range(len(file['text'][d])):
                    for c in range(len(file['text'][d][l])):
                        cont = query(file['text'][d][l][c])
                        if cont:
                            for mas in cont.split(', '):
                                strs= file['text'][d][l][c].split(mas)
                                words = file['text'][d][l][c].split()
                                words1= [strs[0].split(),strs[1].split()]
                                if len(words1[1])>1 and len(words1[0])>1 and (words[len(words)-1] != words1[1][0]) :
                                    if words1[1]:  # Check if words1[1] is not empty
                                        words1[1][0] = words1[0][-1].join(strs[1][0]) if words1[0] else ""  # Check if words1[0] is not empty before accessing its last element
                                    if words1[0]:  # Check if words1[0] is not empty
                                        words1[0] = words1[0][:-1]
                                if len(words1[0])>dist:
                                    results.append("".join(words1[0][-dist:]))
    # map to frequency
    res_set = set(results)
    freq = {}
    for res in res_set:
        freq[res] = results.count(res)
    freq = sorted(freq.items(), key=lambda x: x[1], reverse=True)
    return  freq

def get_left_neighbors(query,dist):
    results = []
    for name in rashi_masechtot:
        file = get_rashi_file(name)
        if file:
            for d in range(len(file['text'])):
                for l in range(len(file['text'][d])):
                    for c in range(len(file['text'][d][l])):
                        cont = query(file['text'][d][l][c])
                        if cont:
                            for mas in cont.split(', '):
                                words = file['text'][d][l][c].split()
                                if mas.strip() in words:
                                    mas_index = words.index(mas.strip())
                                else:
                                    for word in words:
                                        if mas in word:
                                            mas_index = words.index(word)
                                            break
                                if mas_index < len(words) - 1:
                                    word_after_mas = " ".join(words[mas_index + 1:mas_index+1+dist])
                                    results.append(word_after_mas)
    # map to frequency
    res_set = set(results)
    freq = {}
    for res in res_set:
        freq[res] = results.count(res)
    freq = sorted(freq.items(), key=lambda x: x[1], reverse=True)
    return  freq

def find_parallels_to_sheet(sheet_path):
    sheet_cols =["Masechet","Daf","Loc","Text","Term","responding_to"]
    if sheet_path.endswith(".xlsx"):
        sheet_df = pd.read_excel(sheet_path, names=sheet_cols)
    else:
        sheet_df = pd.read_csv(sheet_path, names=sheet_cols,index_col=False)
    sheet_df["daf_index"] = sheet_df["Daf"].apply(daf_to_index)
    sheet_df["ref"] = sheet_df["Masechet"] + " " + sheet_df["Daf"] + ":" + sheet_df["Loc"].str.split(",").str[0]
    output = ""
    for line in shas_refs:
        ref1, ref2 = line
        mas1, loc1a, loc1b = parse_ref(ref1)
        mas2, loc2a, loc2b = parse_ref(ref2)
        if mas1 == mas2:
            continue
        # matches1 = sheet_df[((sheet_df["Masechet"]==mas1) & ((sheet_df["Daf"]==loc1a.split(":")[0]) |
        #                                                      (sheet_df["daf_index"]>=daf_to_index(loc1a.split(":")[0])
        #                                                       & sheet_df["daf_index"]<=daf_to_index(loc1b.split(":")[0]) ))
        #                      )]
        matches1 = sheet_df[
            (sheet_df["Masechet"] == mas1) &
            (
                    (sheet_df["Daf"] == loc1a.split(":")[0]) |
                    sheet_df["ref"].apply(lambda x: ref_in_range(x, ref1))
            )
            ]

        if not matches1.empty:
            # Now check line
            if "-" in loc1a:
                lines= loc1a.split(":")[1].split("-")
                beg, end = int(lines[0]),int(lines[1])
            else:
                beg = int(loc1a.split(":")[1])
                if loc1b:  # if we have a range
                    end = int(loc1b.split(":")[1])
                else:
                    end = beg
            for i,row in matches1.iterrows():
                r_line = int(row["Loc"].split(",")[0]) +1
                if r_line >= beg and r_line <= end:
                    parallel_rashi = get_rashi_text(ref2,join=True)
                    if parallel_rashi:
                        output += row["Masechet"] + row["Daf"]+row["Loc"] #+"\n"
                        output += row["Text"] + "\n"
                        output += ref2 #+ "\n"
                        output += parallel_rashi + "\n"
                    else:
                        print("No Rashi for "+ref2)
        matches2 = sheet_df[
            (sheet_df["Masechet"] == mas2) &
            (
                    (sheet_df["Daf"] == loc2a.split(":")[0]) |
                    sheet_df["ref"].apply(lambda x: ref_in_range(x, ref2))
            )
            ]
        if not matches2.empty:
            # Now check line
            if "-" in loc2a:
                lines= loc2a.split(":")[1].split("-")
                beg, end = int(lines[0]),int(lines[1])
            else:
                beg = int(loc2a.split(":")[1])
                if loc2b:
                    end = int(loc2b.split(":")[1])
                else:
                    end = beg
            for i,row in matches2.iterrows():
                r_line = int(row["Loc"].split(",")[0]) +1
                if r_line >= beg and r_line <= end:
                    parallel_rashi = get_rashi_text(ref1,join=True)
                    if parallel_rashi:
                        output += row["Masechet"] + row["Daf"]+row["Loc"] #+"\n"
                        output += row["Text"] + "\n"
                        output += ref1 #+ "\n"
                        output += parallel_rashi + "\n"
                        # output += "====================================="
                    else:
                        print("No Rashi for "+ref1)

    print("writing file")
    sheet_filename= sheet_path.split("/")[-1].split(".")[0]
    with open(sheet_filename+"_parallels.txt","w") as f:
        f.write(output)

def generate_counts(file_path,col,name):
    if file_path.endswith(".xlsx"):
        names_data= pd.read_excel(file_path)
    else:
        names_data = pd.read_csv(file_path)

    names_counts = defaultdict(lambda: defaultdict(int))
    for i, r in names_data.iterrows():
        masechet = r["Masechet"]
        # if r["Masechet"] not in sums:
        #     sums[r["Masechet"]] = dict()
        if not type(r[col]) == str:
            continue
        for t in r[col].split(","):
            if t not in names_counts[masechet]:
                names_counts[masechet][t] = 0
            names_counts[masechet][t] += 1

    df = pd.DataFrame.from_dict(names_counts, orient='index')
    counts_dir= "/Users/ephraimmeiri/gitEtc/Rashi Thesis work/Searches/counts/"
    df.to_csv(counts_dir+name+"_counts.csv")

from scipy.stats import spearmanr
def compare_orderings(table_path):
    data = pd.read_csv(table_path,index_col="Name")
    data = data.drop(["לא שמעתי","שם","LOG Length (comments)"],axis=1)
    results= dict()
    for name in data.columns:
        for n2 in data.columns:
            if name != n2 and (name,n2) not in results:
                results[(n2,name)]= spearmanr(data[name].rank(),data[n2].rank())
                # print(name,n2)
                # print(spearmanr(data[name],data[n2]))
        # data[name] = data[name].rank()
    # data.to_csv(table_path.split(".")[0]+"_ranked.csv")
    results = [(k[0],k[1],v[0],v[1]) for k,v in results.items()]
    rank_df = pd.DataFrame(results,columns=["F1","F2","Correlation","P"])
    rank_df.to_csv(table_path.split(".")[0]+"_correlation (rank).csv")
    # with open("/Users/ephraimmeiri/gitEtc/Rashi Thesis work/orderings.csv","w") as f:
    #     f.write("F1,F2,Correlation\n")
    #     for r in results:
    #         f.write(r[0]+","+r[1]+","+str(r[2])+"\n")

def count_inversions(df_path):
    df = pd.read_csv(df_path,index_col="Name")
    df = df.drop(["לא שמעתי","שם","LOG Length (comments)"],axis=1)
    results = dict()
    for name in df.columns:
        # Sort Name by column name
        order1= df[name].rank()
        for n2 in df.columns:
            if name != n2:
                order2 = df[n2].rank()
                inversions = 0
                for i in range(len(order1)):
                    for j in range(i+1,len(order1)):
                        if order1[i] > order1[j] and order2[i] < order2[j]:
                            inversions += 1
                print(name,n2,inversions)
                results[(name,n2)] = inversions
    with open("/Users/ephraimmeiri/gitEtc/Rashi Thesis work/inversions1.csv","w") as f:
        f.write("F1,F2,Inversions\n")
        for k,v in results.items():
            f.write(k[0]+","+k[1]+","+str(v)+"\n")
if __name__== "__main__":
    # query = lambda x: "יש " if "יש " in x else None
    # output = get_left_neighbors(query,1)
    # for i in output:
    #     print(i)
    # # print(output)
    #
    text= ' דבר אחר '
    # יש להשיב, יש דריש, יש שלא, יש אומרים, יש שאין, יש דאמר, יש דקתני, יש לשון, יש מפרשים, יש ספרים, יש מפרש, יש גרסינן, יש דאיכא, יש דאית, יש דדריש, יש מקשה, יש אומר, יש גורסין
    output = generate_search(text)
    print(len(output))
    output.to_excel("/Users/ephraimmeiri/gitEtc/Rashi Thesis work/Searches/"+text+".xlsx")

    # all_challanges_path = "/Users/ephraimmeiri/Documents/רשי research docs/רשי תגובה וחידוש/לשונות ביקורת/sifted/‎⁨All_Questions.csv"
    # find_parallels_to_sheet(all_challanges_path)
    #
    #
    # generate_counts("/Users/ephraimmeiri/Documents/רשי research docs/Writing/All_alternates.xlsx","Term","alternates")
    # compare_orderings("/Users/ephraimmeiri/gitEtc/Rashi Thesis work/normalized_3.csv")