import codecs
with codecs.open('signal_engine/volume_monitor.py', 'r', 'utf-8') as f:
    text = f.read()

old_code = """                        if median_p > 0:
                            filtered_exs = [ex_id for ex_id in exs if (full_map[unified][ex_id].get("last_price") or 0) > 0 and 0.25 <= (full_map[unified][ex_id]["last_price"] / median_p) <= 4.0]
                            if len(filtered_exs) >= 2:
                                exs = filtered_exs"""

new_code = """                        if median_p > 0:
                            filtered_exs = [ex_id for ex_id in exs if (full_map[unified][ex_id].get("last_price") or 0) > 0 and 0.6 <= (full_map[unified][ex_id]["last_price"] / median_p) <= 1.6]
                            if len(filtered_exs) >= 2:
                                exs = filtered_exs
                            else:
                                exs = []"""

if old_code in text:
    text = text.replace(old_code, new_code)
    with codecs.open('signal_engine/volume_monitor.py', 'w', 'utf-8') as f:
        f.write(text)
    print('Fixed successfully!')
else:
    print('Old code not found!')
