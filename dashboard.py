import pandas as pd
import pm4py
import streamlit as st
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.visualization.petri_net import visualizer as pn_visualizer
from pm4py.statistics.traces.generic.log import case_statistics
from pm4py.visualization.graphs import visualizer as graphs_visualizer
from datetime import datetime

st.title("Dynamic Process Mining Dashboard")

# Upload CSV or Excel file
uploaded_file = st.file_uploader("Upload Event Log", type=["csv", "xlsx"])

if uploaded_file:
    try:
        # Read file
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file, engine='openpyxl')

        # Display the uploaded data
        st.write("Uploaded Data Preview:", df.head())

        # Allow user to map columns
        st.subheader("🔧 Column Mapping")
        case_id_col = st.selectbox("Select Case ID Column", df.columns)
        activity_col = st.selectbox("Select Activity Column", df.columns)
        timestamp_col = st.selectbox("Select Timestamp Column", df.columns)

        # Convert timestamp column to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
            df[timestamp_col] = pd.to_datetime(df[timestamp_col])

        # Rename columns to PM4Py standard
        df = df.rename(columns={
            case_id_col: 'case:concept:name',
            activity_col: 'concept:name',
            timestamp_col: 'time:timestamp'
        })

        # Data cleaning and preprocessing
        df = pm4py.format_dataframe(df, 
                                  case_id='case:concept:name',
                                  activity_key='concept:name',
                                  timestamp_key='time:timestamp')

        # Filter cases with at least 2 events
        df = pm4py.filter_case_size(df, 2, None)

        # Convert to event log
        event_log = log_converter.apply(df, variant=log_converter.Variants.TO_EVENT_LOG)

        # --------------------------------------------------
        # 1. Process Model Visualization
        # --------------------------------------------------
        st.subheader("🔍 Process Model")
        net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(event_log)
        gviz = pn_visualizer.apply(net, initial_marking, final_marking)
        pn_visualizer.save(gviz, "process_model.png")
        st.image("process_model.png", caption="Discovered Process Model")

        # --------------------------------------------------
        # 2. Key Performance Metrics
        # --------------------------------------------------
        st.subheader("⏱️ Performance Statistics")
        
        # Case duration statistics
        case_durations = case_statistics.get_all_case_durations(event_log)
        avg_duration = sum(case_durations) / len(case_durations) if case_durations else 0
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Avg Case Duration", f"{round(avg_duration/3600, 2)} hours")
        col2.metric("Min Case Duration", f"{round(min(case_durations)/3600, 2)} hours" if case_durations else "N/A")
        col3.metric("Max Case Duration", f"{round(max(case_durations)/3600, 2)} hours" if case_durations else "N/A")

        # --------------------------------------------------
        # 3. Activity Frequency and Timing Analysis
        # --------------------------------------------------
        st.subheader("📊 Activity Frequency and Timing Analysis")
        
        # Get basic frequency counts
        activity_counts = pm4py.get_event_attribute_values(event_log, "concept:name")
        
        # Initialize dictionary to store timing information
        activity_timing = {}
        
        # Calculate timing statistics for each activity
        for trace in event_log:
            previous_event = None
            
            # Sort events in trace by timestamp to ensure correct order
            sorted_events = sorted(trace, key=lambda x: x['time:timestamp'])
            
            for event in sorted_events:
                activity = event['concept:name']
                if activity not in activity_timing:
                    activity_timing[activity] = {'durations': []}
                
                # Calculate duration from previous activity (if exists)
                if previous_event is not None:
                    duration = (event['time:timestamp'] - previous_event['time:timestamp']).total_seconds()
                    activity_timing[activity]['durations'].append(duration)
                
                previous_event = event
        
        # Create DataFrame with all metrics
        activity_data = []
        for activity in activity_counts.keys():
            durations = activity_timing[activity]['durations']
            
            activity_data.append({
                'Activity': activity,
                'Count': activity_counts[activity],
                'Percentage (%)': round(activity_counts[activity] / sum(activity_counts.values()) * 100, 2),
                'Min Duration (hours)': round(min(durations)/3600, 2) if durations else 0,
                'Avg Duration (hours)': round(sum(durations)/len(durations)/3600, 2) if durations else 0,
                'Max Duration (hours)': round(max(durations)/3600, 2) if durations else 0,
                'Sample Size': len(durations)  # Added to show how many duration measurements we have
            })
        
        activity_df = pd.DataFrame(activity_data).sort_values('Count', ascending=False)

        # Display bar chart for frequency
        st.bar_chart(activity_df.set_index('Activity')['Count'])
        
        # Display comprehensive table
        st.subheader("Activity Details Table")
        st.dataframe(activity_df, hide_index=True)
        
        # Add download button
        @st.cache_data
        def convert_df_to_csv(df):
            return df.to_csv(index=False).encode('utf-8')
        
        csv = convert_df_to_csv(activity_df)
        st.download_button(
            label="📥 Download Activity Analysis Data",
            data=csv,
            file_name="activity_analysis.csv",
            mime="text/csv",
        )

        # --------------------------------------------------
        # 4. Process Variants (with Average Duration)
        # --------------------------------------------------
        st.subheader("🔄 Process Variants")
        
        # Get variants using current PM4Py API
        variants = pm4py.get_variants_as_tuples(event_log)
        
        variant_stats = []
        for variant, traces in variants.items():
            variant_durations = []
            for trace in traces:
                # Get the first and last event timestamps for the trace
                events = list(filter(lambda x: 'time:timestamp' in x, trace))
                if len(events) > 1:
                    start_time = events[0]['time:timestamp']
                    end_time = events[-1]['time:timestamp']
                    duration = (end_time - start_time).total_seconds()
                    variant_durations.append(duration)
            
            avg_duration = sum(variant_durations) / len(variant_durations) if variant_durations else 0
            variant_stats.append({
                'Variant': ' → '.join(variant),
                'Count': len(traces),
                'Avg Duration (hours)': round(avg_duration / 3600, 2)
            })

        variants_df = pd.DataFrame(variant_stats).sort_values('Count', ascending=False)
        
        # Display all variants (not just top 10)
        st.dataframe(variants_df, hide_index=True)

        # --------------------------------------------------
        # 5. Visualize Each Variant in the Process Model
        # --------------------------------------------------
        st.subheader("🔍 Visualize Each Variant in the Process Model")
        
        # Allow user to select a variant
        selected_variant = st.selectbox("Select a Variant", variants_df['Variant'].tolist())
        
        # Filter event log for the selected variant
        filtered_log = pm4py.filter_variants(event_log, [tuple(selected_variant.split(' → '))])
        
        # Discover Petri net for the selected variant
        net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(filtered_log)
        gviz = pn_visualizer.apply(net, initial_marking, final_marking)
        pn_visualizer.save(gviz, "variant_process_model.png")
        st.image("variant_process_model.png", caption=f"Process Model for Variant: {selected_variant}")

        # --------------------------------------------------
        # 6. Bottleneck Analysis (Improved)
        # --------------------------------------------------
        st.subheader("🐢 Bottleneck Analysis")
        
        # Get performance DFG (returns tuple: frequency_dfg, performance_dfg)
        perf_dfg = pm4py.discover_performance_dfg(event_log)
        mean_times = perf_dfg[1]  # Access the PERFORMANCE metrics

        performance_data = []
        for key, time_info in mean_times.items():
            # PM4Py returns performance metrics as dictionary for intervals
            if isinstance(time_info, dict):
                avg_time = time_info.get('mean', 0)
            else:  # Handle legacy format
                avg_time = time_info
            
            # Ensure numeric value and filter valid entries
            if isinstance(avg_time, (int, float)) and avg_time > 0:
                performance_data.append({
                    'From': key[0],  # First activity
                    'To': key[1],    # Second activity
                    'Avg Time (hours)': round(avg_time / 3600, 2)
                })

        if performance_data:
            perf_df = pd.DataFrame(performance_data)
            perf_df = perf_df.sort_values('Avg Time (hours)', ascending=False)
            
            # Display bottleneck analysis with explanation
            st.write("Bottleneck Analysis identifies transitions between activities with the longest average durations.")
            st.dataframe(perf_df, hide_index=True)
            
            # Visualize bottleneck transitions
            st.write("### Bottleneck Transitions")
            st.write("The following transitions take the longest time:")
            for _, row in perf_df.head(5).iterrows():
                st.write(f"- **{row['From']} → {row['To']}**: {row['Avg Time (hours)']} hours")
        else:
            st.warning("No valid performance data available for bottleneck analysis")

        st.success("All process mining metrics generated successfully!")

    except Exception as e:
        st.error(f"Error: {str(e)}")
        # Add detailed error information for debugging
        st.error("Detailed error information:")
        import traceback
        st.code(traceback.format_exc())

# cd C:\ProcessMiningApp        or  cd C:\Python Scripts\Process Mining
# streamlit run dashboard.py
#Your browser will automatically open at http://localhost:8501