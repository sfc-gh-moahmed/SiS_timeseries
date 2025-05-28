import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, lit
from datetime import datetime

# --- Streamlit Page Configuration ---
st.set_page_config(page_title="Batch Data Editor", layout="wide")

# --- Configuration ---
TABLE_NAME = "DEMO.DT_TUTORIAL.SYNTHETIC_BATCH_DATA"
PK_COLUMN = "BATCH_ID"
EDITOR_KEY = "batch_editor"

# --- Snowflake Session ---
try:
    session = get_active_session()
    st.success("❄️ Connected to Snowflake!")
except Exception:
    st.error("Could not get active Snowflake session. Ensure you are running this in Snowflake.")
    st.stop()

# --- Helper Functions ---

def get_data(_session, table_name, pk_column):
    """Fetches data from Snowflake and converts to Pandas."""
    st.write(f"Fetching data from `{table_name}`...")
    try:
        snow_df = _session.table(table_name).order_by(col(pk_column).asc()).limit(100)
        pdf = snow_df.to_pandas()
        if pdf.empty:
            st.warning(f"Table '{table_name}' appears to be empty.")
            # Define columns if empty based on your known schema
            pdf = pd.DataFrame(columns=[PK_COLUMN, 'BATCH_NAME', 'PROJECT', 'TIMESTAMP', 'PERSON'])
        elif pk_column not in pdf.columns:
            st.error(f"PK '{pk_column}' not found. Please check config.")
            st.stop()

        if 'TIMESTAMP' in pdf.columns:
            pdf['TIMESTAMP'] = pd.to_datetime(pdf['TIMESTAMP'])
        pdf = pdf.set_index(pk_column, drop=False)
        st.write(f"Fetched {len(pdf)} rows.")
        return pdf
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame().set_index(PK_COLUMN, drop=False)

def get_changes_from_editor(editor_key, original_df, pk_column):
    """Extracts additions, edits, and deletions."""
    if editor_key not in st.session_state: return None
    changes = st.session_state[editor_key]
    added_rows = changes.get("added_rows", [])
    edited_rows = changes.get("edited_rows", [])
    deleted_rows_indices = changes.get("deleted_rows", [])

    added_df = pd.DataFrame(added_rows)
    if 'TIMESTAMP' in added_df.columns:
        added_df['TIMESTAMP'] = pd.to_datetime(added_df['TIMESTAMP'], errors='coerce')

    changes_dict = {"added": added_df, "edited": {}, "deleted": pd.DataFrame()}

    if edited_rows:
        edited_pks = [original_df.index[i] for i in edited_rows.keys()]
        original_edited = original_df.loc[edited_pks].copy()
        new_edited = original_edited.copy()
        for idx, changes_for_row in edited_rows.items():
            pk_val = original_df.index[idx]
            for col_name, new_val in changes_for_row.items():
                new_edited.loc[pk_val, col_name] = new_val
        if 'TIMESTAMP' in new_edited.columns:
            new_edited['TIMESTAMP'] = pd.to_datetime(new_edited['TIMESTAMP'], errors='coerce')
        changes_dict["edited"] = {"original": original_edited, "new": new_edited}

    if deleted_rows_indices:
        deleted_pks = [original_df.index[i] for i in deleted_rows_indices]
        changes_dict["deleted"] = original_df.loc[deleted_pks]

    return changes_dict

def apply_changes(_session, table_name, pk_column, changes):
    """Applies changes to Snowflake table."""
    snow_table = _session.table(table_name)
    results = {"added": 0, "edited": 0, "deleted": 0, "errors": []}
    try:
        # Deletions
        if not changes["deleted"].empty:
            deleted_ids = changes["deleted"][pk_column].tolist()
            if deleted_ids:
                delete_result = snow_table.delete(col(pk_column).in_(deleted_ids))
                results["deleted"] = delete_result.rows_deleted
        # Updates
        if changes["edited"]:
            edited_df = changes["edited"]["new"]
            for pk_id, row in edited_df.iterrows():
                update_dict = row.drop(pk_column).to_dict() # Exclude PK
                for k, v in update_dict.items():
                    if pd.isna(v): update_dict[k] = None
                    elif isinstance(v, pd.Timestamp): update_dict[k] = v.to_pydatetime()
                try:
                    update_result = snow_table.update(update_dict, col(pk_column) == pk_id)
                    results["edited"] += update_result.rows_updated
                except Exception as e: results["errors"].append(f"Update {pk_id}: {e}")
        # Additions
        if not changes["added"].empty:
            added_df_for_insert = changes["added"].drop(columns=[pk_column], errors='ignore')
            try:
                snow_df_to_add = _session.create_dataframe(added_df_for_insert)
                snow_df_to_add.write.mode("append").save_as_table(table_name)
                results["added"] = len(added_df_for_insert)
            except Exception as e: results["errors"].append(f"Add: {e}")
    except Exception as e: results["errors"].append(f"General: {e}")
    return results

# NEW: Function to create a styling DataFrame for highlighting changes
def style_changed_cells(new_df, original_df):
    """Creates a Styler DF to highlight changes between two DataFrames."""
    style_df = pd.DataFrame('', index=new_df.index, columns=new_df.columns)
    # Ensure original_df is aligned and has same columns for comparison
    original_comp = original_df.reindex(new_df.index)

    for idx in new_df.index:
        for col_name in new_df.columns:
            new_val = new_df.loc[idx, col_name]
            try:
                original_val = original_comp.loc[idx, col_name]
                # Compare considering NaT/NaN as equal
                if (pd.isna(new_val) and pd.isna(original_val)):
                    changed = False
                else:
                    changed = new_val != original_val
                
                if changed:
                    style_df.loc[idx, col_name] = 'background-color: #ffff99' # Yellow highlight
            except KeyError:
                 # Should not happen if DFs are aligned, but handle defensively
                 style_df.loc[idx, col_name] = 'background-color: #ffcccc' # Red if error
    return style_df

# --- Streamlit App UI ---
st.title("📊 Batch Data Editor & Confirmer")
st.sidebar.write(f"Edit data in `{TABLE_NAME}`. PK: `{PK_COLUMN}`.")
st.sidebar.warning(f"`{PK_COLUMN}` is auto-generating & cannot be edited.")

# --- Initialize Session State ---
if 'original_data' not in st.session_state:
    st.session_state.original_data = get_data(session, TABLE_NAME, PK_COLUMN)
    st.session_state.show_confirmation = False
    st.session_state.changes_to_confirm = None
    st.session_state.editor_instance_id = 0

# Ensure it exists even if 'original_data' does (for subsequent runs/older state)
if 'editor_instance_id' not in st.session_state:
    st.session_state.editor_instance_id = 0

# --- Main Data Editor ---
st.subheader("Edit Batch Data Here")
st.caption("Edit/Add/Delete rows, then click 'Review Changes'.")

current_key = f"{EDITOR_KEY}_{st.session_state.editor_instance_id}"

edited_df = st.data_editor(
    st.session_state.original_data.copy(), 
    key=current_key, 
    num_rows="dynamic",
    use_container_width=True, disabled=[PK_COLUMN],
    column_config={
        "TIMESTAMP": st.column_config.DatetimeColumn("Timestamp", format="YYYY-MM-DD HH:mm:ss"),
    }
)

if st.button("Review Changes"):
    current_key = f"{EDITOR_KEY}_{st.session_state.editor_instance_id}"
    st.session_state.changes_to_confirm = get_changes_from_editor(
        current_key, st.session_state.original_data, PK_COLUMN
    )
    changes = st.session_state.changes_to_confirm
    if (changes["added"].empty and not changes["edited"] and changes["deleted"].empty):
        st.warning("No changes detected.")
        st.session_state.show_confirmation = False
    else:
        st.session_state.show_confirmation = True
        st.info("Changes detected. Please review below.")
        st.rerun()

# --- Confirmation Section ---
if st.session_state.get('show_confirmation', False) and st.session_state.changes_to_confirm:
    changes = st.session_state.changes_to_confirm
    st.markdown("---")
    st.subheader("Please Confirm Your Changes")
    has_changes = False

    if not changes["deleted"].empty:
        st.warning("Rows to be DELETED:")
        st.dataframe(changes["deleted"], use_container_width=True)
        has_changes = True

    if not changes["added"].empty:
        st.info("Rows to be ADDED:")
        st.dataframe(changes["added"], use_container_width=True)
        has_changes = True

    # --- MODIFIED: Show Edited Rows Separately ---
    if changes["edited"]:
        st.info("Rows to be EDITED:")
        
        # Box 1: Original State
        st.markdown("##### Original State:")
        st.dataframe(changes["edited"]["original"], use_container_width=True)

        # Box 2: New State with Highlights
        st.markdown("##### New State (Changes Highlighted):")
        new_edited_df = changes["edited"]["new"]
        original_edited_df = changes["edited"]["original"]
        
        # Apply the new styling function
        styled_df = new_edited_df.style.apply(
            lambda df: style_changed_cells(df, original_edited_df), 
            axis=None
        )
        st.dataframe(styled_df, use_container_width=True)
        
        has_changes = True
    # --- END MODIFICATION ---

    if not has_changes:
         st.warning("Changes were not detected. Please review again.")
    else:
        col1, col2 = st.columns(2)
        with col1:
            if st.button("✅ Confirm and Save Changes", type="primary", use_container_width=True):
                with st.spinner("Applying changes..."):
                    results = apply_changes(session, TABLE_NAME, PK_COLUMN, changes)
                    st.success("Changes applied!")
                    st.write(results)
                    # Clear state and refresh
                    st.session_state.original_data = get_data(session, TABLE_NAME, PK_COLUMN)
                    st.session_state.show_confirmation = False
                    st.session_state.changes_to_confirm = None

                    # Clean up current key and increment
                    current_key = f"{EDITOR_KEY}_{st.session_state.editor_instance_id}"
                    if current_key in st.session_state: 
                        del st.session_state[current_key]
                    st.session_state.editor_instance_id += 1
                    
                    st.balloons()
                    st.rerun()
        with col2:
            if st.button("❌ Cancel", use_container_width=True):
                st.info("Changes Canceled. Editor reverted.")
                # Clear confirmation state
                st.session_state.show_confirmation = False
                st.session_state.changes_to_confirm = None
                
                # Force new editor instance by changing key 
                current_key = f"{EDITOR_KEY}_{st.session_state.editor_instance_id}"
                if current_key in st.session_state:
                    del st.session_state[current_key]
                st.session_state.editor_instance_id += 1 # Increment ID for new key
                
                st.rerun()

# --- Expander for Raw Data ---
st.markdown("---")
expander = st.expander("See Current 100 Records from Database")
with expander:
    st.dataframe(st.session_state.original_data, use_container_width=True)