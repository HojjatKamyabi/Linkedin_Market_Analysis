import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
import os
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_config():
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")
    
    if not user or not password:
        st.error("Database credentials (DB_USER, DB_PASSWORD) are missing. Please configure them.")
        st.stop()
        
    return {
        "host": os.environ.get("DB_HOST", "postgres"),
        "port": os.environ.get("DB_PORT", "5432"),
        "dbname": os.environ.get("DB_NAME", "linkedin_market"),
        "user": user,
        "password": password
    }

@st.cache_data(ttl=60)
def load_jobs():
    config = get_db_config()
    conn = None
    try:
        conn = psycopg2.connect(**config)
        query = """
            SELECT 
                linkedin_job_id, 
                title, 
                company, 
                location, 
                posting_date, 
                required_skills, 
                seniority_level, 
                llm_model 
            FROM jobs
        """
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        logger.error(f"Database query failed: {e}")
        st.error("Failed to connect or query the database.")
        st.stop()
    finally:
        if conn is not None:
            conn.close()

def normalize_skills(skills_data):
    if not skills_data:
        return []
    
    if isinstance(skills_data, str):
        try:
            skills_data = json.loads(skills_data)
        except Exception:
            return []
            
    if not isinstance(skills_data, list):
        return []
        
    normalized = []
    seen = set()
    for skill in skills_data:
        if isinstance(skill, str) and skill.strip():
            skill_clean = skill.strip()
            skill_lower = skill_clean.lower()
            if skill_lower not in seen:
                seen.add(skill_lower)
                normalized.append(skill_clean)
                
    return normalized

def apply_filters(df, selected_companies, selected_seniority, search_text):
    filtered_df = df.copy()
    
    if selected_companies:
        filtered_df = filtered_df[
            filtered_df["company"].fillna("").isin(selected_companies)
        ]
        
    if selected_seniority:
        filtered_df = filtered_df[
            filtered_df["seniority_level"].fillna("").isin(selected_seniority)
        ]
        
    search_text = search_text.strip()
    
    if search_text:
        search_mask = pd.Series(False, index=filtered_df.index)
        
        for column in ["title", "company", "location"]:
            values = filtered_df[column].fillna("").astype(str)
            
            search_mask |= values.str.contains(
                search_text,
                case=False,
                regex=False,
                na=False,
            )
            
        filtered_df = filtered_df[search_mask]
        
    return filtered_df

def render_metrics(df, unique_skills):
    col1, col2, col3, col4 = st.columns(4)
    
    total_jobs = len(df)
    unique_companies = df['company'].nunique() if not df.empty else 0
    unique_skills_count = len(unique_skills)
    
    if not df.empty and 'seniority_level' in df.columns:
        valid_seniority = df['seniority_level'].dropna()
        most_common_seniority = valid_seniority.mode()[0] if not valid_seniority.empty else "N/A"
    else:
        most_common_seniority = "N/A"
        
    with col1:
        st.metric("Total Jobs", total_jobs)
    with col2:
        st.metric("Unique Companies", unique_companies)
    with col3:
        st.metric("Unique Technical Skills", unique_skills_count)
    with col4:
        st.metric("Most Common Seniority", most_common_seniority)

def main():
    st.set_page_config(page_title="LinkedIn Job Market Analysis", layout="wide", page_icon="💼")
    
    st.title("LinkedIn Job Market Analysis")
    st.markdown("Public LinkedIn job listings enriched with structured skill and seniority analysis.")
    
    with st.sidebar:
        st.header("Filters")
        if st.button("Refresh Data"):
            st.cache_data.clear()
            st.rerun()
            
    df = load_jobs()
    
    if df.empty:
        st.info("No jobs data found in the database.")
        st.stop()
        
    df['parsed_skills'] = df['required_skills'].apply(normalize_skills)
    df['Skills Display'] = df['parsed_skills'].apply(lambda x: ", ".join(x) if x else "N/A")
    
    all_companies = sorted(df['company'].dropna().unique().tolist())
    all_seniority = sorted(df['seniority_level'].dropna().unique().tolist())
    
    with st.sidebar:
        selected_companies = st.multiselect("Company", options=all_companies, default=[])
        selected_seniority = st.multiselect("Seniority", options=all_seniority, default=[])
        search_text = st.text_input("Search (Title, Company, Location)")
        
    filtered_df = apply_filters(df, selected_companies, selected_seniority, search_text)
    
    all_filtered_skills_lower = {}
    for skills_list in filtered_df['parsed_skills']:
        for skill in skills_list:
            skill_lower = skill.lower()
            if skill_lower not in all_filtered_skills_lower:
                all_filtered_skills_lower[skill_lower] = {"name": skill, "count": 1}
            else:
                all_filtered_skills_lower[skill_lower]["count"] += 1
                
    render_metrics(filtered_df, all_filtered_skills_lower)
    
    if filtered_df.empty:
        st.warning("No jobs match the current filters.")
        st.stop()
        
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Jobs by Seniority")
        seniority_counts = filtered_df['seniority_level'].value_counts().reset_index()
        seniority_counts.columns = ['Seniority', 'Count']
        if not seniority_counts.empty:
            fig_seniority = px.bar(
                seniority_counts, 
                x='Seniority', 
                y='Count',
                title="Jobs by Seniority"
            )
            st.plotly_chart(fig_seniority, width="stretch")
        else:
            st.info("No seniority data available.")
            
    with col2:
        st.subheader("Top Hiring Companies")
        company_counts = filtered_df['company'].dropna().value_counts().reset_index()
        company_counts.columns = ['Company', 'Count']
        if not company_counts.empty:
            top_companies = company_counts.head(10)
            fig_company = px.bar(
                top_companies,
                x='Company',
                y='Count',
                title="Top Hiring Companies"
            )
            st.plotly_chart(fig_company, width="stretch")
        else:
            st.info("No company data available.")
            
    st.markdown("---")
    st.subheader("Top Technical Skills")
    if all_filtered_skills_lower:
        skills_df = pd.DataFrame(list(all_filtered_skills_lower.values()))
        skills_df = skills_df.sort_values(by='count', ascending=False).head(15)
        fig_skills = px.bar(
            skills_df,
            x='count',
            y='name',
            orientation='h',
            title="Top Technical Skills"
        )
        fig_skills.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig_skills, width="stretch")
    else:
        st.info("No technical skills data available.")
        
    st.markdown("---")
    st.subheader("Jobs Table")
    
    display_df = filtered_df[[
        'title', 'company', 'location', 'posting_date', 
        'seniority_level', 'Skills Display', 'llm_model'
    ]].copy()
    
    display_df.columns = [
        'Title', 'Company', 'Location', 'Posting Date', 
        'Seniority', 'Required Skills', 'LLM Model'
    ]
    
    if 'Posting Date' in display_df.columns:
        display_df = display_df.sort_values(by='Posting Date', ascending=False, na_position='last')
        
    st.dataframe(display_df, width="stretch")

if __name__ == "__main__":
    main()
