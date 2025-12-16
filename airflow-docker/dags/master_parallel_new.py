from airflow.sdk import dag, task, Param
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
import logging
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'hakkache',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(hours=2),
}

@dag(
    dag_id='master_parallel_sentiment_analysis',
    default_args=default_args,
    description='Master DAG to trigger multiple parallel sentiment analysis pipelines',
    schedule=None,
    start_date=datetime(2025, 12, 16),
    catchup=False,
    tags=['master', 'parallel', 'sentiment-analysis', 'orchestration'],
    params={
        'total_rows': Param(
            default=1000,
            type='integer',
            description='Total number of rows to process across all parallel runs'
        ),
        'parallel_runs': Param(
            default=10,
            type='integer',
            description='Number of parallel DAG runs to execute'
        ),
        'input_file': Param(
            default='/opt/airflow/input/yelp_academic_dataset_review.json',
            type='string',
            description='Path to the input JSON file'
        ),
        'ollama_model': Param(
            default='llama3.2',
            type='string',
            description='Ollama model to use for sentiment analysis'
        ),
    },
    render_template_as_native_obj=True
)
def master_parallel_pipeline():
    
    @task
    def calculate_batches(**context):
        """Calculate batch sizes and offsets for parallel runs"""
        params = context['params']
        total_rows = params.get('total_rows', 1000)
        parallel_runs = params.get('parallel_runs', 10)
        
        # Calculate batch size per run
        batch_size = total_rows // parallel_runs
        remainder = total_rows % parallel_runs
        
        batches = []
        current_offset = 0
        
        for i in range(parallel_runs):
            # Distribute remainder across first few runs
            current_batch_size = batch_size + (1 if i < remainder else 0)
            
            batches.append({
                'run_id': i,
                'offset': current_offset,
                'batch_size': current_batch_size
            })
            
            current_offset += current_batch_size
        
        logger.info(f"Calculated {len(batches)} batches for {total_rows} total rows")
        logger.info(f"Batch configuration: {json.dumps(batches, indent=2)}")
        
        return batches
    
    @task
    def create_trigger_configs(batches, **context):
        """Create trigger configurations for each parallel run"""
        params = context['params']
        input_file = params.get('input_file', '/opt/airflow/input/yelp_academic_dataset_review.json')
        ollama_model = params.get('ollama_model', 'llama3.2')
        
        trigger_configs = []
        
        for batch in batches:
            config = {
                'input_file': input_file,
                'batch_size': batch['batch_size'],
                'offset': batch['offset'],
                'ollama_model': ollama_model
            }
            trigger_configs.append(config)
        
        logger.info(f"Created {len(trigger_configs)} trigger configurations")
        return trigger_configs
    
    # Calculate batches and configs
    batches = calculate_batches()
    trigger_configs = create_trigger_configs(batches)
    
    @task
    def verify_configs(trigger_configs):
        """Verify each config has unique offset"""
        logger.info("=== PARALLEL EXECUTION PLAN ===")
        for i, config in enumerate(trigger_configs):
            logger.info(f"Run {i}: offset={config['offset']}, batch_size={config['batch_size']}, "
                       f"processes rows {config['offset']} to {config['offset'] + config['batch_size'] - 1}")
        logger.info(f"Total runs: {len(trigger_configs)}")
        return trigger_configs
    
    verified_configs = verify_configs(trigger_configs)
    
    # Create individual trigger operators for true parallelization
    # Each trigger processes a DIFFERENT section of data with unique offset
    # wait_for_completion=True ensures master DAG waits for child DAG success
    trigger_0 = TriggerDagRunOperator(
        task_id='trigger_run_0',
        trigger_dag_id='self_healing_agentic_sentiment_analysis_pipeline',
        conf="{{ ti.xcom_pull(task_ids='verify_configs')[0] if ti.xcom_pull(task_ids='verify_configs')|length > 0 else {} }}",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed']
    )
    
    trigger_1 = TriggerDagRunOperator(
        task_id='trigger_run_1',
        trigger_dag_id='self_healing_agentic_sentiment_analysis_pipeline',
        conf="{{ ti.xcom_pull(task_ids='verify_configs')[1] if ti.xcom_pull(task_ids='verify_configs')|length > 1 else {} }}",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed']
    )
    
    trigger_2 = TriggerDagRunOperator(
        task_id='trigger_run_2',
        trigger_dag_id='self_healing_agentic_sentiment_analysis_pipeline',
        conf="{{ ti.xcom_pull(task_ids='verify_configs')[2] if ti.xcom_pull(task_ids='verify_configs')|length > 2 else {} }}",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed'],
        trigger_rule='all_done'
    )
    
    trigger_3 = TriggerDagRunOperator(
        task_id='trigger_run_3',
        trigger_dag_id='self_healing_agentic_sentiment_analysis_pipeline',
        conf="{{ ti.xcom_pull(task_ids='verify_configs')[3] if ti.xcom_pull(task_ids='verify_configs')|length > 3 else {} }}",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed'],
        trigger_rule='all_done'
    )
    
    trigger_4 = TriggerDagRunOperator(
        task_id='trigger_run_4',
        trigger_dag_id='self_healing_agentic_sentiment_analysis_pipeline',
        conf="{{ ti.xcom_pull(task_ids='verify_configs')[4] if ti.xcom_pull(task_ids='verify_configs')|length > 4 else {} }}",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed'],
        trigger_rule='all_done'
    )
    
    trigger_5 = TriggerDagRunOperator(
        task_id='trigger_run_5',
        trigger_dag_id='self_healing_agentic_sentiment_analysis_pipeline',
        conf="{{ ti.xcom_pull(task_ids='verify_configs')[5] if ti.xcom_pull(task_ids='verify_configs')|length > 5 else {} }}",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed'],
        trigger_rule='all_done'
    )
    
    trigger_6 = TriggerDagRunOperator(
        task_id='trigger_run_6',
        trigger_dag_id='self_healing_agentic_sentiment_analysis_pipeline',
        conf="{{ ti.xcom_pull(task_ids='verify_configs')[6] if ti.xcom_pull(task_ids='verify_configs')|length > 6 else {} }}",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed'],
        trigger_rule='all_done'
    )
    
    trigger_7 = TriggerDagRunOperator(
        task_id='trigger_run_7',
        trigger_dag_id='self_healing_agentic_sentiment_analysis_pipeline',
        conf="{{ ti.xcom_pull(task_ids='verify_configs')[7] if ti.xcom_pull(task_ids='verify_configs')|length > 7 else {} }}",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed'],
        trigger_rule='all_done'
    )
    
    trigger_8 = TriggerDagRunOperator(
        task_id='trigger_run_8',
        trigger_dag_id='self_healing_agentic_sentiment_analysis_pipeline',
        conf="{{ ti.xcom_pull(task_ids='verify_configs')[8] if ti.xcom_pull(task_ids='verify_configs')|length > 8 else {} }}",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed'],
        trigger_rule='all_done'
    )
    
    trigger_9 = TriggerDagRunOperator(
        task_id='trigger_run_9',
        trigger_dag_id='self_healing_agentic_sentiment_analysis_pipeline',
        conf="{{ ti.xcom_pull(task_ids='verify_configs')[9] if ti.xcom_pull(task_ids='verify_configs')|length > 9 else {} }}",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=['success'],
        failed_states=['failed'],
        trigger_rule='all_done'
    )
    
    @task
    def generate_summary(**context):
        """Generate summary of triggered runs - only runs if all child DAGs succeeded"""
        params = context['params']
        
        summary = {
            'timestamp': datetime.utcnow().isoformat(),
            'total_rows': params.get('total_rows'),
            'parallel_runs': params.get('parallel_runs'),
            'status': 'All DAG runs completed successfully',
            'batch_size_per_run': params.get('total_rows') // params.get('parallel_runs')
        }
        
        logger.info("=" * 80)
        logger.info("PARALLEL PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"  Total Rows Processed: {summary['total_rows']}")
        logger.info(f"  Parallel Runs: {summary['parallel_runs']}")
        logger.info(f"  Batch Size Per Run: {summary['batch_size_per_run']}")
        logger.info(f"  All child DAGs completed with SUCCESS status")
        logger.info("=" * 80)
        
        return summary
    
    # Set dependencies - all triggers run in parallel after configs are verified
    # Master DAG only succeeds if ALL child DAGs succeed (wait_for_completion=True)
    verified_configs >> [trigger_0, trigger_1, trigger_2, trigger_3, trigger_4, 
                         trigger_5, trigger_6, trigger_7, trigger_8, trigger_9]
    
    [trigger_0, trigger_1, trigger_2, trigger_3, trigger_4, 
     trigger_5, trigger_6, trigger_7, trigger_8, trigger_9] >> generate_summary()

master_parallel_pipeline_dag = master_parallel_pipeline()

