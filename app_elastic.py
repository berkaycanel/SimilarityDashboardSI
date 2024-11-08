import streamlit as st
import pandas as pd
import es
import re
import base64

st.set_page_config(layout="wide")

def get_encoded_logo(logo_path):
    with open(logo_path, "rb") as image_file:
        encoded_image = image_file.read()
    return encoded_image

logo_image = get_encoded_logo("si_logo.png")  
col1, col2 = st.columns([8, 3])  

with col1:
    st.title("Domain Similarity Dashboard")

with col2:
    st.image(logo_image, width=350)

def format_tags(tags_list, as_dropdown=False):
    tags_html = ''
    for tag in tags_list:
        tags_html += f'<span class="tag">{tag}</span> '
    tags_html = tags_html.strip()
    if as_dropdown and tags_html:
        tags_html = f'<details><summary>Show Tags</summary>{tags_html}</details>'
    return tags_html

def generate_input_table(fields_dict):
    table_html = '<table>'
    for key, value in fields_dict.items():
        table_html += f'<tr><th>{key}</th><td>{value}</td></tr>'
    table_html += '</table>'
    return table_html

def inject_custom_css():
    css = """
    <style>
    .tag {
        display: inline-block;
        background-color: #e0f7fa;
        color: #006064;
        padding: 5px 10px;
        margin: 3px 3px 3px 0;
        border-radius: 15px;
        font-size: 14px;
    }
    table {
        width: 100%;
        border-collapse: collapse;
        table-layout: fixed;
    }
    th, td {
        word-wrap: break-word;
        vertical-align: top;
        padding: 10px;
        border: 1px solid #ddd;
    }
    th {
        text-align: left;
        background-color: #OE1117;
        width: 25%;
    }
    td a {
        color: #1a0dab;
        text-decoration: none;
    }
    td a:hover {
        text-decoration: underline;
    }
    /* Style for the dropdown */
    details {
        cursor: pointer;
    }
    details summary {
        font-weight: bold;
        color: #006064;
        margin-bottom: 5px;
    }
    details[open] summary {
        margin-bottom: 0;
    }
    details > * {
        margin-left: 15px;
    }
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)

def clean_matched_values(values):
    cleaned_values = [re.sub(r'</?em>', '', v) for v in values] 
    return cleaned_values

def convert_to_list_format(text):
    text = re.sub(r'<.*?>', '', text)
    
    tags = text.split(",")
    
    tags = [tag.strip() for tag in tags if tag.strip()]
    
    return ', '.join(tags)

@st.cache_data
def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

def strip_html_tags(text):
    clean = re.compile('<.*?>')
    cleaned_text = re.sub(clean, '', text) if isinstance(text, str) else text
    return cleaned_text.replace("Show Tags", "").strip()

def display_domain_info(domain):
    input_data = es.get_domain_tags_new(domain)
    
    if not input_data:
        st.error(f"No data found for domain: {domain}")
        return

    input_fields = {
        'Refined GPT Tags': input_data.get('refined_gpt_tags', []),
        'CB Tags': input_data.get('cb_tags', []),
        'LI Tags': input_data.get('li_tags', []),
        'Funding Stage': input_data.get('funding_stage', 'N/A'),
        'Employees': input_data.get('employees', 'N/A'),
        'Total Funding Amount': input_data.get('total_funding_amount', 'N/A'),
        'WP Tags': input_data.get('wp_tags', []),
        'id': input_data.get('id', 'N/A')
    }
    
    display_input_fields = {}
    for key, value in input_fields.items():
        display_input_fields[key] = format_tags(value) if isinstance(value, list) else value

    table_html = generate_input_table(display_input_fields)
    inject_custom_css()

    st.subheader(f"Input Fields for Domain: {domain}")
    st.markdown(table_html, unsafe_allow_html=True)

    results = es.get_related_domains_new(
        refined_gpt_tags=input_fields['Refined GPT Tags'],
        cb_tags=input_fields['CB Tags'],
        li_tags=input_fields['LI Tags'],
        wp_tags=input_fields['WP Tags'],
        domain=domain, 
        funding_stage=input_fields['Funding Stage'], 
        employees=input_fields['Employees'], 
        total_funding_amount=input_fields['Total Funding Amount']
    )

    domains, scores, matched_refined_gpt_tags, matched_cb_tags, matched_li_tags, matched_wp_tags = [], [], [], [], [], []
    matched_funding_stage, matched_employees, matched_total_funding_amount = [], [], []

    for result in results:
        domain_res = result['_source']['domain']
        score = result['_score']
        domain_link = f'<a href="https://{domain_res}" target="_blank">{domain_res}</a>'

        refined_gpt_matches, cb_matches, li_matches, wp_matches = [], [], [], []
        funding_stage_match, employees_match, total_funding_match = [], [], []

        if 'highlight' in result:
            for field, values in result['highlight'].items():
                cleaned_values = clean_matched_values(values)

                if 'refined_gpt_tags.keyword' in field:
                    refined_gpt_matches.extend(cleaned_values)
                elif 'cb_tags.keyword' in field:
                    cb_matches.extend(cleaned_values)
                elif 'li_tags.keyword' in field:
                    li_matches.extend(cleaned_values)
                elif 'wp_tags.keyword' in field:
                    wp_matches.extend(cleaned_values)
                elif 'funding_stage' in field:
                    funding_stage_match.extend(cleaned_values)
                elif 'employees' in field:
                    employees_match.extend(cleaned_values)
                elif 'total_funding_amount' in field:
                    total_funding_match.extend(cleaned_values)

        domains.append(domain_link)
        scores.append(score)
        matched_refined_gpt_tags.append(format_tags(refined_gpt_matches, as_dropdown=True) if refined_gpt_matches else '')
        matched_cb_tags.append(format_tags(cb_matches, as_dropdown=True) if cb_matches else '')
        matched_li_tags.append(format_tags(li_matches, as_dropdown=True) if li_matches else '')
        matched_wp_tags.append(format_tags(wp_matches, as_dropdown=True) if wp_matches else '')
        matched_funding_stage.append(', '.join(funding_stage_match) if funding_stage_match else '')
        matched_employees.append(', '.join(employees_match) if employees_match else '')
        matched_total_funding_amount.append(', '.join(total_funding_match) if total_funding_match else '')

    similarity_df = pd.DataFrame({
        'Domain': domains,
        'Score': scores,
        'Matched Refined GPT Tags': matched_refined_gpt_tags,
        'Matched CB Tags': matched_cb_tags,
        'Matched LI Tags': matched_li_tags,
        'Matched WP Tags': matched_wp_tags,
        'Matched Funding Stage': matched_funding_stage,
        'Matched Employees': matched_employees,
        'Matched Total Funding Amount': matched_total_funding_amount
    })

    st.subheader(f"Similarity Results for Domain: {domain}")
    st.write(similarity_df.to_html(escape=False, index=False), unsafe_allow_html=True)


    download_df = similarity_df.copy()
    download_df['Domain'] = download_df['Domain'].apply(strip_html_tags)
    download_df['Matched Refined GPT Tags'] = download_df['Matched Refined GPT Tags'].apply(lambda x: convert_to_list_format(strip_html_tags(x)))
    download_df['Matched CB Tags'] = download_df['Matched CB Tags'].apply(lambda x: convert_to_list_format(strip_html_tags(x)))
    download_df['Matched LI Tags'] = download_df['Matched LI Tags'].apply(lambda x: convert_to_list_format(strip_html_tags(x)))
    download_df['Matched WP Tags'] = download_df['Matched WP Tags'].apply(lambda x: convert_to_list_format(strip_html_tags(x)))
    
    similarity_csv = convert_df_to_csv(download_df)
    st.download_button(
        label="Download Similarity Data as CSV",
        data=similarity_csv,
        file_name=f'similarity_data_{domain}.csv',
        mime='text/csv',
    )

@st.cache_data
def get_domain_list():
    return es.get_all_domains()

domain_list = get_domain_list()
domain_count = len(domain_list) 
st.write(f"Number of selectable domains: {domain_count}")

if domain_list:
    domain_list.sort()
    domain_list.insert(0, 'Select a domain')
    domain_input = st.selectbox("Select a domain to analyze", domain_list)
    if domain_input != 'Select a domain':
        display_domain_info(domain_input)
else:
    st.error("Unable to retrieve domain list from the database.")
