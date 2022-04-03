import pandas as pd
import zipfile
import os
from datetime import datetime


sp_info = pd.read_csv("./species_information.csv")
# sp_info = sp_info[sp_info["Category"] != "domestic"]

sp_list = tuple(sp_info[sp_info["Category"] == "species"]["Scientific Name"])
endm_sp_list = tuple(sp_info[sp_info["Endemic"] == "endemic species"]["Scientific Name"])
endm_subsp_list = tuple(sp_info[sp_info["Endemic"] == "endemic subspecies"]["Scientific Name"])
dss_sp_list = tuple(sp_info[sp_info["Da Shue Shan"] == 1]["Scientific Name"])
race_sp_list = tuple(sp_info[sp_info["Race"] == 1]["Scientific Name"])


def concat_csv_in_unzip(path):

    pb_files = os.listdir(path)
    n_csv = 0
    get_csv = False

    for fn in pb_files:

        if ".csv" in fn:
            n_csv += 1

            get_dt = pd.read_csv(path + "/" + fn[:-4] + ".csv")
            get_dt["Group"] = path.split("/")[-1]
            get_dt["Team Name"] = fn[:-4]

            if not get_csv:
                dt = get_dt
                get_csv = True

            else:
                frames = [dt, get_dt]
                dt = pd.concat(frames)



    if n_csv == 0:

        for fn in pb_files:

            with zipfile.ZipFile(path + "/" + fn, 'r') as zip_ref:
                zip_ref.extractall(path)
                csv_fn = fn[:-4] + ".csv"
                os.rename(path + "/MyEBirdData.csv", path + "/" + csv_fn)

            get_dt = pd.read_csv(path + "/" + fn[:-4] + ".csv")
            get_dt["Group"] = path.split("/")[-1]
            get_dt["Team Name"] = fn[:-4]

            if not get_csv:
                dt = get_dt
                get_csv = True

            else:
                frames = [dt, get_dt]
                dt = pd.concat(frames)

    dt = pd.merge(dt, sp_info[["Scientific Name", "Category"]], how="left", on="Scientific Name")

    return dt


def data_summary(dt, dt_type = "global"):

    dt_sum = {}

    sp_dt = dt[dt["Category"].isin(["issf", "species", "intergrade", "form", "domestic"])]

    # global total number species, endemic subspecies, endemic species, number of individual
    all_sp_list = set([i.split(" ")[0] + " " + i.split(" ")[1] for i in sp_dt["Scientific Name"]])
    # all_sp_list = set(sp_dt["Scientific Name"])
    dt_sum["n_sp"] = len(all_sp_list)
    dt_sum["n_endm_sp"] = len(all_sp_list & set(endm_sp_list))
    dt_sum["n_endm_subsp"] = len(all_sp_list & set(endm_subsp_list))
    dt_sum["n_indv"] = dt[dt["Count"] != "X"]["Count"].astype(int).sum()

    if dt_type == "global":

        # weird species alert
        w_sp = {
            "only1": [],
            "only2": [],
            "only3": [],
        }

        w_sp["fir_rec_sp"] = list(all_sp_list - set(dss_sp_list))

        ti_saw = list(all_dt[all_dt["Category"].isin(["domestic", "form", "intergrade", "issf", "species"])][["Scientific Name", "Team Name"]].drop_duplicates()["Scientific Name"])

        for sp in all_sp_list:
            if ti_saw.count(sp) == 1:
                w_sp["only1"].append(sp)
            if ti_saw.count(sp) == 2:
                w_sp["only2"].append(sp)
            if ti_saw.count(sp) == 3:
                w_sp["only3"].append(sp)

        dt_sum["w_sp"] = w_sp

    return dt_sum

def group_rank(group_dt):

    teams = tuple(set(group_dt["Team Name"]))

    group_result = {
        "Team Name": [],
        "Number of Species": [],
        "Number of Endemic Species": [],
        "Number of Endemic Subspecies": [],
        "Number of Individual": [],
    }

    for t in teams:
        team_dt = group_dt[group_dt["Team Name"] == t]
        team_sum = data_summary(team_dt, dt_type= "team")

        group_result["Team Name"].append(t)
        group_result["Number of Species"].append(team_sum["n_sp"])
        group_result["Number of Endemic Species"].append(team_sum["n_endm_sp"])
        group_result["Number of Endemic Subspecies"].append(team_sum["n_endm_subsp"])
        group_result["Number of Individual"].append(team_sum["n_indv"])

    group_result = pd.DataFrame(group_result)

    return group_result

def sp_alert_info(sp: str):

    unrecd_dt = all_dt[all_dt["Scientific Name"] == sp]

    alert_md = ""

    for i, row in unrecd_dt.iterrows():
        s_id = "\n- **紀錄清單** %s" % row["Submission ID"]
        dt = "\n    - 時間：%s" % row["Date"] + " " + row["Time"]
        loc = "\n   - 地點：%s" % row["Location"]
        team = "\n  - 觀察隊伍：%s" % row["Team Name"]
        url = "https://ebird.org/checklist/" + row["Submission ID"]
        link = "\n  - [清單連結](%s)" % url

        alert_md = alert_md +s_id + dt + loc + team + link + "\n"

    return alert_md

if __name__ == "__main__":



    pb_path = "./public-group"
    pc_path = "./parent-child-group"
    pb_dt = concat_csv_in_unzip(pb_path)
    pc_dt = concat_csv_in_unzip(pc_path)

    pb_dt_c_names = []
    pb_dt_s_names = []
    pc_dt_c_names = []
    pc_dt_s_names = []

    for co, s, ca in zip(pb_dt["Common Name"], pb_dt["Scientific Name"], pb_dt["Category"]):
        if ca in ["domestic", "form", "issf", "intergrade", "issf"]:
            pb_dt_c_names.append(co.split("(")[0])
            pb_dt_s_names.append(s.split(" ")[0] + " " + s.split(" ")[1])
        else:
            pb_dt_c_names.append(co)
            pb_dt_s_names.append(s)

    for co, s, ca in zip(pc_dt["Common Name"], pc_dt["Scientific Name"], pc_dt["Category"]):
        if ca in ["domestic", "form", "issf", "intergrade", "issf"]:
            pc_dt_c_names.append(co.split("(")[0])
            pc_dt_s_names.append(s.split(" ")[0] + " " + s.split(" ")[1])
        else:
            pc_dt_c_names.append(co)
            pc_dt_s_names.append(s)

    pb_dt["Common Name"] = pb_dt_c_names
    pb_dt["Scientific Name"] = pb_dt_s_names
    pc_dt["Common Name"] = pc_dt_c_names
    pc_dt["Scientific Name"] = pc_dt_s_names

    # global summary
    all_dt = pd.concat([pb_dt, pc_dt])
    glo_sum = data_summary(all_dt, dt_type="global")

    # group summary
    pb_result = group_rank(pb_dt)
    pc_result = group_rank(pc_dt)

    pb_result.to_csv("./公開組成績.csv", index = False)
    pc_result.to_csv("./親子組成績.csv", index = False)

    # write markdown result
    time_now = datetime.now().strftime("%Y-%m-%d_%H%M")
    output_path = "./bird_race_result_%s.txt" % time_now
    output_f = open(output_path, "w", encoding='UTF-8')

    year_now = datetime.now().strftime("%Y")
    glo_result = "# %s 大雪山賞鳥大賽比賽結果\n## 大會總紀錄\n- 鳥種數：%s\n- 特有種：%s\n- 特有亞種：%s\n- 鳥隻數：%s" % (year_now, glo_sum["n_sp"], glo_sum["n_endm_sp"], glo_sum["n_endm_subsp"], glo_sum["n_indv"])

    output_f.write(glo_result)

    output_f.write("\n## 公開組")
    output_f.write("\n### 鳥種數排名")

    output_f.write("\n| 名次 | 隊伍 | 鳥種數 | 特有種數 |")
    output_f.write("\n| --- | --- | --- | --- |")

    for i, row in pb_result.sort_values(by=["Number of Species", "Number of Endemic Species", "Number of Endemic Subspecies", "Number of Individual"], ascending=False).reset_index().iterrows():
        if i == 5:
            break
        rank = i + 1
        output_f.write("\n| 第%s名 | %s | %s | %s |"% (rank, row["Team Name"], row["Number of Species"], row["Number of Endemic Species"]))


    output_f.write("\n## 親子組")
    output_f.write("\n### 鳥種數排名")

    output_f.write("\n| 名次 | 隊伍 | 鳥種數 | 特有種數 |")
    output_f.write("\n| --- | --- | --- | --- |")

    for i, row in pc_result.sort_values(by=["Number of Species", "Number of Endemic Species", "Number of Endemic Subspecies", "Number of Individual"], ascending=False).reset_index().iterrows():
        if i == 5:
            break
        rank = i + 1
        output_f.write("\n| 第%s名 | %s | %s | %s |"% (rank, row["Team Name"], row["Number of Species"], row["Number of Endemic Species"]))


    output_f.write("\n## 其他獎項")

    output_f.write("\n### 特有種獎")

    output_f.write("\n| 名次 | 隊伍 | 特有種數 | 特有種亞數 |")
    output_f.write("\n| --- | --- | --- | --- |")

    all_result = pd.concat([pb_result, pc_result])

    for i, row in all_result.sort_values(by=["Number of Endemic Species", "Number of Endemic Subspecies"], ascending=False).reset_index().iterrows():
        if i == 5:
            break
        rank = i + 1
        output_f.write("\n| 第%s名 | %s | %s | %s |"% ( rank, row["Team Name"], row["Number of Endemic Species"], row["Number of Endemic Subspecies"]))

    output_f.write("\n### 新紀錄種獎")

    output_f.write("\n| 隊伍 | 新紀錄鳥種 | 英文名 |")
    output_f.write("\n| --- | --- | --- |")

    for sp in glo_sum["w_sp"]["fir_rec_sp"]:
        teams = set(all_dt[all_dt["Scientific Name"] == sp]["Team Name"])

        c_name = sp_info.loc[sp_info["Scientific Name"] == sp, "Common Name"].item()
        e_name = sp_info.loc[sp_info["Scientific Name"] == sp, "English Common Name"].item()

        for t in teams:
            output_f.write("\n| %s | %s | %s |"% (t, c_name, e_name))


    output_f.write("\n## 須注意的紀錄")
    output_f.write("\n### 未曾記錄過的物種")

    for sp in glo_sum["w_sp"]["fir_rec_sp"]:

        c_name = sp_info.loc[sp_info["Scientific Name"] == sp, "Common Name"].item()
        e_name = sp_info.loc[sp_info["Scientific Name"] == sp, "English Common Name"].item()

        output_f.write("\n#### %s %s" % (c_name, e_name))
        output_f.write(sp_alert_info(sp))


    output_f.write("\n### 只有一隊看到的物種")

    for sp in glo_sum["w_sp"]["only1"]:

        c_name = sp_info.loc[sp_info["Scientific Name"] == sp, "Common Name"].item()
        e_name = sp_info.loc[sp_info["Scientific Name"] == sp, "English Common Name"].item()
        output_f.write("\n#### %s %s" % (c_name, e_name))
        output_f.write(sp_alert_info(sp))

    output_f.write("\n### 只有兩隊看到的物種")

    for sp in glo_sum["w_sp"]["only2"]:
        c_name = sp_info.loc[sp_info["Scientific Name"] == sp, "Common Name"].item()
        e_name = sp_info.loc[sp_info["Scientific Name"] == sp, "English Common Name"].item()
        output_f.write("\n#### %s %s" % (c_name, e_name))
        output_f.write(sp_alert_info(sp))

    output_f.write("\n### 只有三隊看到的物種")

    for sp in glo_sum["w_sp"]["only3"]:

        c_name = sp_info.loc[sp_info["Scientific Name"] == sp, "Common Name"].item()
        e_name = sp_info.loc[sp_info["Scientific Name"] == sp, "English Common Name"].item()
        output_f.write("\n#### %s %s" % (c_name, e_name))
        output_f.write(sp_alert_info(sp))