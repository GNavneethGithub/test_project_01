
"""
Phase 1: Stuck Records Handler
Handles stuck records in in_progress state with selective cleanup and reset
"""

from typing import Dict, Any, List

def handle_stuck_records_in_progress_state(config: Dict[str, Any]):
    """
    Phase 1: Handle stuck records with selective cleanup and reset
    Complete Airflow task for stuck record recovery
    
    Args:
        config: Configuration dictionary containing all necessary parameters
    """
    
    # Step 1: Find all stale records (stuck in in_progress for more than acceptable time)
    stuck_records = get_all_in_progress_records_that_cross_the_threshold(config)
    
    for old_record in stuck_records:
        
        # Step 2: Process each stuck record
        # Identify where the pipeline got stuck
        stuck_stage_info = identify_stuck_stage_in_pipeline_flow_order(config, old_record)
        
        # Send alert about stuck process
        send_stuck_process_alert_email(config, old_record, stuck_stage_info)
        
        # Clean up partial data from stuck stage only
        cleanup_partial_data_for_stuck_stage(config, old_record, stuck_stage_info["stuck_stage"])
        
        # Create new record with reset values
        new_record = reset_stuck_record_column_values(config, old_record, stuck_stage_info)
        
        # Replace old record with new record in Snowflake
        delete_and_upload_this_record_to_snowflake_drive_table(config, new_record, old_record)




