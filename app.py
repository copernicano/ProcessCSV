import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from collections import defaultdict

st.set_page_config(page_title="CSV Column Sum Calculator", layout="wide")

st.title("💼 Calcolatore Somma Colonne CSV")
st.write("Carica un file CSV, seleziona le colonne da sommare e opzionalmente raggruppa per una colonna")

# Inizializza le variabili di stato
if 'file_analyzed' not in st.session_state:
    st.session_state.file_analyzed = False
if 'headers' not in st.session_state:
    st.session_state.headers = []
if 'numeric_columns' not in st.session_state:
    st.session_state.numeric_columns = []
if 'sample_data' not in st.session_state:
    st.session_state.sample_data = None
if 'calculation_requested' not in st.session_state:
    st.session_state.calculation_requested = False
if 'selected_columns' not in st.session_state:
    st.session_state.selected_columns = []
if 'group_by_column' not in st.session_state:
    st.session_state.group_by_column = None

# Funzione per analizzare il file
def analyze_file():
    try:
        st.session_state.uploaded_file.seek(0)
        sample = pd.read_csv(
            st.session_state.uploaded_file, 
            nrows=10,
            delimiter=st.session_state.delimiter,
            thousands=st.session_state.thousands_sep if st.session_state.thousands_sep else None,
            decimal=st.session_state.decimal_sep,
            encoding=st.session_state.encoding
        )
        
        st.session_state.headers = sample.columns.tolist()
        st.session_state.numeric_columns = [col for col in st.session_state.headers 
                                          if pd.api.types.is_numeric_dtype(sample[col])]
        st.session_state.categorical_columns = [col for col in st.session_state.headers 
                                             if col not in st.session_state.numeric_columns]
        st.session_state.sample_data = sample
        st.session_state.file_analyzed = True
        return True
    except Exception as e:
        st.error(f"Si è verificato un errore durante l'analisi del file: {str(e)}")
        st.info("Suggerimento: prova a cambiare l'encoding o il delimitatore nelle opzioni di caricamento.")
        st.session_state.file_analyzed = False
        return False

# Funzione per calcolare le somme, con supporto al raggruppamento
def calculate_sums():
    if not st.session_state.selected_columns:
        st.warning("Seleziona almeno una colonna da sommare")
        return False
    
    try:
        # Reinizializziamo il file per il caricamento completo
        st.session_state.uploaded_file.seek(0)
        
        # Calcoliamo i totali utilizzando chunks per file di grandi dimensioni
        chunk_size = 100000
        
        # Definiamo le colonne da leggere
        usecols = st.session_state.selected_columns.copy()
        
        # Se c'è una colonna di raggruppamento, includiamola
        has_group_by = st.session_state.group_by_column is not None and st.session_state.group_by_column != "Nessun raggruppamento"
        if has_group_by:
            usecols.append(st.session_state.group_by_column)
        
        # Inizializziamo i contatori
        if has_group_by:
            # Dizionario di dizionari per tenere traccia dei totali per gruppo
            group_sums = defaultdict(lambda: {col: 0.0 for col in st.session_state.selected_columns})
            group_counts = defaultdict(lambda: {col: 0 for col in st.session_state.selected_columns})
            # Per tenere traccia di tutti i gruppi
            all_groups = set()
        else:
            # Contatori semplici se non c'è raggruppamento
            total_sums = {col: 0.0 for col in st.session_state.selected_columns}
            valid_counts = {col: 0 for col in st.session_state.selected_columns}
        
        # Creiamo una progress bar
        progress_bar = st.progress(0)
        
        # Leggiamo il file a blocchi
        chunks = pd.read_csv(
            st.session_state.uploaded_file, 
            usecols=usecols, 
            chunksize=chunk_size,
            delimiter=st.session_state.delimiter,
            thousands=st.session_state.thousands_sep if st.session_state.thousands_sep else None,
            decimal=st.session_state.decimal_sep,
            encoding=st.session_state.encoding
        )
        
        for i, chunk in enumerate(chunks):
            # Se abbiamo un raggruppamento, processiamo i dati per gruppo
            if has_group_by:
                # Iteriamo sui gruppi in questo chunk
                grouped = chunk.groupby(st.session_state.group_by_column)
                for group_name, group_data in grouped:
                    # Aggiungiamo il gruppo alla lista di tutti i gruppi
                    all_groups.add(group_name)
                    
                    # Per ogni colonna da sommare
                    for col in st.session_state.selected_columns:
                        # Convertiamo a numerico se necessario
                        if col not in st.session_state.numeric_columns:
                            group_data[col] = pd.to_numeric(group_data[col], errors='coerce')
                        
                        # Calcoliamo la somma per questo gruppo e colonna
                        col_sum = group_data[col].sum(skipna=True)
                        if not pd.isna(col_sum):
                            group_sums[group_name][col] += col_sum
                            group_counts[group_name][col] += group_data[col].count()
            else:
                # Processamento standard senza raggruppamento
                for col in st.session_state.selected_columns:
                    # Convertiamo a numerico se necessario
                    if col not in st.session_state.numeric_columns:
                        chunk[col] = pd.to_numeric(chunk[col], errors='coerce')
                    
                    # Sommiamo solo i valori non-NaN
                    col_sum = chunk[col].sum(skipna=True)
                    if not pd.isna(col_sum):
                        total_sums[col] += col_sum
                        valid_counts[col] += chunk[col].count()
            
            # Aggiorniamo la progress bar
            progress_bar.progress(min((i+1)/(i+10), 0.99))
        
        progress_bar.progress(1.0)  # Completiamo la progress bar
        
        # Prepariamo i risultati
        if has_group_by:
            # Risultati raggruppati
            results = []
            
            # Per ogni gruppo
            for group_name in sorted(all_groups):
                # Per ogni colonna sommare
                for col in st.session_state.selected_columns:
                    results.append({
                        "Gruppo": group_name,
                        "Colonna": col,
                        "Somma": group_sums[group_name][col],
                        "Valori validi": group_counts[group_name][col]
                    })
        else:
            # Risultati non raggruppati
            results = []
            for col in st.session_state.selected_columns:
                results.append({
                    "Colonna": col,
                    "Somma": total_sums[col],
                    "Valori validi": valid_counts[col]
                })
        
        st.session_state.results_df = pd.DataFrame(results)
        st.session_state.grouped_results = has_group_by
        st.session_state.calculation_done = True
        return True
        
    except Exception as e:
        st.error(f"Si è verificato un errore durante il calcolo delle somme: {str(e)}")
        st.session_state.calculation_done = False
        return False

# Opzioni di caricamento PRIMA del caricamento del file
st.subheader("1️⃣ Opzioni di caricamento")
st.session_state.delimiter = st.text_input("Delimitatore", value=",")
st.session_state.thousands_sep = st.text_input("Separatore migliaia", value="")
st.session_state.decimal_sep = st.text_input("Separatore decimale", value=".")
st.session_state.encoding = st.selectbox("Encoding del file", 
                                       options=["utf-8", "latin1", "ISO-8859-1", "cp1252"], 
                                       index=0)

# File uploader per caricare il file CSV
st.subheader("2️⃣ Carica il tuo file")
st.session_state.uploaded_file = st.file_uploader("Carica un file CSV", type="csv")

# Gestione del flusso dell'applicazione
if st.session_state.uploaded_file is not None:
    # Pulsante per analizzare il file con le impostazioni specificate
    if not st.session_state.file_analyzed:
        if st.button("🔍 Analizza file"):
            analyze_file()
    
    # Se il file è stato analizzato con successo, mostriamo i risultati dell'analisi
    if st.session_state.file_analyzed:
        st.success(f"✅ File analizzato con successo! Rilevate {len(st.session_state.headers)} colonne, di cui {len(st.session_state.numeric_columns)} numeriche.")
        
        # Mostriamo un'anteprima dei dati
        st.subheader("Anteprima dei dati")
        st.dataframe(st.session_state.sample_data)
        
        # Passo 3: Selezione delle colonne e raggruppamento
        st.subheader("3️⃣ Configura l'analisi")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Selezione colonne da sommare
            st.session_state.selected_columns = st.multiselect(
                "Seleziona le colonne da sommare:",
                options=st.session_state.headers,
                default=st.session_state.numeric_columns[:min(5, len(st.session_state.numeric_columns))]
            )
        
        with col2:
            # Selezione colonna per raggruppamento
            group_options = ["Nessun raggruppamento"] + st.session_state.categorical_columns
            st.session_state.group_by_column = st.selectbox(
                "Raggruppa per colonna (opzionale):",
                options=group_options,
                index=0,
                help="Seleziona una colonna per raggruppare i risultati"
            )
            
            # Convertiamo "Nessun raggruppamento" a None
            if st.session_state.group_by_column == "Nessun raggruppamento":
                st.session_state.group_by_column = None
        
        if st.session_state.selected_columns:
            # Verifichiamo quali delle colonne selezionate sono numeriche
            non_numeric_selected = [col for col in st.session_state.selected_columns 
                                   if col not in st.session_state.numeric_columns]
            if non_numeric_selected:
                st.warning(f"⚠️ Attenzione: le seguenti colonne non sembrano essere numeriche: {', '.join(non_numeric_selected)}. L'app tenterà di convertirle.")
            
            # Bottone per avviare il calcolo
            if st.button("🧮 Calcola somme"):
                with st.spinner("Elaborazione in corso... Potrebbe richiedere tempo per file grandi."):
                    st.session_state.calculation_requested = True
                    calculate_sums()
        
        # Mostro i risultati se il calcolo è stato richiesto e completato
        if st.session_state.get('calculation_requested', False) and st.session_state.get('calculation_done', False):
            # Visualizziamo i risultati
            st.subheader("Risultati")
            
            # Verifico se ci sono risultati raggruppati
            has_groups = st.session_state.get('grouped_results', False)
            
            # Mostriamo la tabella dei risultati
            if has_groups:
                # Configurazione colonne per risultati raggruppati
                st.dataframe(
                    st.session_state.results_df,
                    column_config={
                        "Gruppo": st.column_config.TextColumn("Gruppo", help="Valore di raggruppamento"),
                        "Colonna": st.column_config.TextColumn("Colonna", help="Colonna sommata"),
                        "Somma": st.column_config.NumberColumn("Somma", format="%.2f"),
                        "Valori validi": st.column_config.NumberColumn("Valori validi", format="%d")
                    },
                    hide_index=True
                )
                
                # Creiamo un grafico a barre raggruppate
                pivot_df = st.session_state.results_df.pivot(index='Gruppo', columns='Colonna', values='Somma')
                
                # Reset index per avere la colonna "Gruppo" come colonna normale
                plot_df = pivot_df.reset_index()
                
                # Riformattiamo per Plotly
                melted_df = pd.melt(plot_df, id_vars=['Gruppo'], var_name='Colonna', value_name='Somma')
                
                fig = px.bar(
                    melted_df, 
                    x='Gruppo', 
                    y='Somma', 
                    color='Colonna',
                    title="Somma per gruppo e colonna",
                    labels={"Somma": "Valore Totale", "Gruppo": "Valore Gruppo"}
                )
                st.plotly_chart(fig, use_container_width=True)
                
            else:
                # Configurazione colonne per risultati non raggruppati
                st.dataframe(
                    st.session_state.results_df,
                    column_config={
                        "Colonna": st.column_config.TextColumn("Colonna", help="Colonna sommata"),
                        "Somma": st.column_config.NumberColumn("Somma", format="%.2f"),
                        "Valori validi": st.column_config.NumberColumn("Valori validi", format="%d")
                    },
                    hide_index=True
                )
                
                # Creiamo un grafico a barre semplice
                fig = px.bar(
                    st.session_state.results_df, 
                    x="Colonna", 
                    y="Somma",
                    title="Somma per colonna",
                    labels={"Somma": "Valore totale", "Colonna": "Nome colonna"}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Offriamo il download dei risultati
            csv = st.session_state.results_df.to_csv(index=False)
            st.download_button(
                label="📥 Scarica risultati come CSV",
                data=csv,
                file_name="risultati_somme.csv",
                mime="text/csv"
            )
    
else:
    # Messaggio informativo quando nessun file è caricato
    st.info("👆 Carica un file CSV per iniziare.")
    
    # Esempio di formattazione del file
    st.subheader("Il tuo file CSV dovrebbe avere un formato simile:")
    example_data = """
    categoria,prodotto,quantità,prezzo,totale
    Frutta,Mele,10,2.5,25
    Frutta,Pere,15,3.2,48
    Frutta,Banane,8,1.8,14.4
    Verdura,Carote,12,1.5,18
    Verdura,Pomodori,20,2.0,40
    """
    st.code(example_data)
    
    st.info("💡 Con questo tool potrai calcolare somme semplici o raggruppate, ad esempio il totale delle vendite per categoria.")