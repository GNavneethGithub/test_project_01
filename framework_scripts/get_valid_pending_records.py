

from typing import Dict, Any, List

def get_N_valid_executable_records(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Phase 2: Get N valid executable records based on time window accessibility
    
    Args:
        config: Configuration containing batch size and acceptable time intervals
        
    Returns:
        List of valid records that can actually be executed
    """
    
    # Step 1: Get N pending records (batch processing)
    N_pending_records = get_N_pending_pipeline_status_records(config)
    
    valid_executable_records = []
    
    for old_record in N_pending_records:
        
        # Step 2: Check if query window is within acceptable time interval
        time_window_acceptable = check_query_window_within_acceptable_time_interval(config, old_record)
        
        if time_window_acceptable:
            
            # Step 3: Check if historical data is accessible for this record
            historical_data_accessible = check_if_historical_data_is_accessible_for_record(config, old_record)
            
            if historical_data_accessible:
                # Record is valid and executable
                valid_executable_records.append(old_record)
            else:
                # Historical data not accessible - create new record and update
                new_record = create_failed_permanently_record_for_historical_data_not_accessible(config, old_record)
                update_drive_table_record(config, old_record, new_record)
        else:
            # Time window outside acceptable range - create new record and update
            new_record = create_failed_permanently_record_for_time_window_outside_range(config, old_record)
            update_drive_table_record(config, old_record, new_record)
    
    return valid_executable_records




def check_and_handle_already_processed_records(config: Dict[str, Any], valid_executable_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Phase 3: Check if records are already processed and handle partial/complete processing
    
    Args:
        config: Configuration dictionary
        valid_executable_records: Records from Phase 2
        
    Returns:
        List of records that need actual processing (not already completed)
    """
    
    records_needing_processing = []
    
    # Process current batch of records
    for old_record in valid_executable_records:
        
        # Check if source and target audit is enabled
        if old_record.get("source_and_target_audit_enabled", False):
            
            # Get current source and target counts
            current_source_count = get_current_source_count(config, old_record)
            current_target_count = get_current_target_count(config, old_record)
            
            # Check if counts match (pipeline already processed)
            if current_source_count == current_target_count:
                
                # Pipeline already processed successfully - update record as completed
                new_record = create_completed_record_with_current_timestamps_and_counts(config, old_record, current_source_count, current_target_count)
                update_drive_table_record(config, old_record, new_record)
                
            else:
                
                # Counts don't match - check what's been completed and clean up partial data
                process_completion_status = check_which_processes_have_been_completed(config, old_record)
                
                # Clean up partial data based on completion status
                cleanup_partial_data_based_on_process_completion_status(config, old_record, process_completion_status)
                
                # Record needs processing
                records_needing_processing.append(old_record)
        
        else:
            
            # Source and target audit not enabled - can't verify completion, assume needs processing
            records_needing_processing.append(old_record)
    
    return records_needing_processing



def get_required_records_to_process(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    
    all_records_needing_processing = []
    expected_N = get_expected_batch_size_from_config(config)
    previous_pending_count = None
    max_iterations = 10
    current_iteration = 0
    
    while len(all_records_needing_processing) < expected_N and current_iteration < max_iterations:
        
        current_iteration += 1
        
        # Phase 2: Get N valid executable records (filters out future/invalid records)
        current_batch = get_N_valid_executable_records(config)
        current_pending_count = len(current_batch)
        
        # Safety 1: Stop if pending count same as previous iteration
        if previous_pending_count == current_pending_count:
            break
        
        # Phase 3: Check already processed from the valid records
        records_needing_processing = check_and_handle_already_processed_records(config, current_batch)
        all_records_needing_processing.extend(records_needing_processing)
        
        # Update for next iteration
        previous_pending_count = current_pending_count
        
        # Safety 2: Break if no more records
        if current_pending_count == 0:
            break
    
    return all_records_needing_processing[:expected_N]



