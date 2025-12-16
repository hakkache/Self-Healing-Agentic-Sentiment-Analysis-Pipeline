from airflow.sdk import dag,task,Param,get_current_context
import logging 
import os
from datetime import datetime, timedelta
import itertools
import json
import re

logger = logging.getLogger(__name__)

class Config:
    BASE_DIR = os.getenv('PIPELINE_BASE_DIR', '/opt/airflow')
    INPUT_FILE = os.getenv('PIPELINE_INPUT_FILE', '/opt/airflow/input/yelp_academic_dataset_review.json')
    OUTPUT_DIR = os.getenv('PIPELINE_OUTPUT_DIR', '/opt/airflow/output/')
    MAX_TEXT_LENGTH = int(os.getenv('PIPELINE_MAX_TEXT_LENGTH', 2000))
    DEFAULT_BATCH_SIZE = 100
    DEFAULT_OFFSET = 0
    OLLAMA_HOST = os.getenv('OLLAMA_HOST', 'http://host.docker.internal:11434')
    OLLAMA_MODEL = os.getenv('OLLAMA_MODEL', 'llama3.2')
    OLLAMA_TIMEOUT = int(os.getenv('OLLAMA_TIMEOUT', 120))
    OLLAMA_RETRIES = int(os.getenv('OLLAMA_RETRIES', 3))

default_args = {
    'owner': 'hakkache',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=60),
    'max_active_runs': 10,
}

def _load_ollama_model(model_name: str):
    import ollama
    logger.info(f"Loading Ollama model: {model_name}")
    logger.info(f"Ollama host: {Config.OLLAMA_HOST}")

    client = ollama.Client(host=Config.OLLAMA_HOST)

    try :
        client.show(model_name)
        logger.info(f"Model {model_name} is available.")
    except Exception as e:
        logger.error(f"Error checking model {model_name}: {e}")
        try:
            client.pull(model_name)
            logger.info(f"Model {model_name} pulled successfully.")
        except Exception as pull_error:
            logger.error(f"Error pulling model {model_name}: {pull_error}")
            raise

    test_response = client.chat(
        model = model_name,
        messages = [{"role": "user", "content": "Classify the sentiment : 'This is Great product' as positive , negative , or neutral"}])

    test_result = test_response['message']['content'].strip().upper()
    logger.info(f"Test response from model {model_name}: {test_result}")   

    return {
        'backend': 'ollama',
        'model_name': model_name,
        'ollama_host': Config.OLLAMA_HOST,
        'max_length': Config.MAX_TEXT_LENGTH,
        'status': 'loaded',
        'validated_at': datetime.utcnow().isoformat()
    }

def _load_from_file(params: dict, batch_size: int, offset: int):
    input_file = params.get('input_file', Config.INPUT_FILE)

    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file {input_file} does not exist.")
    
    reviews = []
    with open(input_file, 'r', encoding='utf-8') as f:
        sliced = itertools.islice(f, offset,offset + batch_size)

        for line in sliced:
            try :
                review = json.loads(line.strip())
                reviews.append(
                    {
                        'review_id': review.get('review_id'),
                        'business_id': review.get('business_id'),
                        'user_id': review.get('user_id'),
                        'stars': review.get('stars',0),
                        'text': review.get('text'),
                        'date': review.get('date'),
                        'useful': review.get('useful',0),
                        'funny': review.get('funny',0),
                        'cool': review.get('cool',0)
                    }
                )
            except json.JSONDecodeError as e:
                logger.warning(f"Skipping invalid JSON line: {e}")
                continue
        logger.info(f"Loaded {len(reviews)} reviews from offset {offset} with batch size {batch_size}.")
        return reviews    


def _parse_ollama_response(response: str):
    try:
        clean_text = response.strip()

        if clean_text.startswith('```') :
            line = clean_text.split('\n')
            clean_text = '\n'.join(line[1:-1]) if line[-1].strip() == '```' else '\n'.join(line[1:])
        
        parsed = json.loads(clean_text)
        sentiment = parsed.get('sentiment','NEUTRAL').upper()
        confidence = float(parsed.get('confidence',0.0))

        if sentiment not in ['POSITIVE','NEGATIVE','NEUTRAL']:
            sentiment = 'NEUTRAL'
        
        return {
            'label': sentiment,
            'score': min(max(confidence,0.0),1.0)

        }
    
    except (json.JSONDecodeError, ValueError,KeyError,TypeError) as e:
        upper_text = response.strip().upper()
        if 'POSITIVE' in upper_text:
            return {'label':'POSITIVE','score':0.75}
        elif 'NEGATIVE' in upper_text:
            return {'label':'NEGATIVE','score':0.75}
        else:
            return {'label':'NEUTRAL','score':0.50}

def _heal_review(review: dict)-> dict:
    text = review.get('text','')
    
    result = {
        'review_id': review.get('review_id'),
        'business_id': review.get('business_id'),
        'stars': review.get('stars',0),
        'original_text': None,
        'error_type': None,
        'action_taken': 'none',
        'was_healed': False,
        'metadata': {
            'user_id': review.get('user_id'),
            'date': review.get('date'),
            'useful': review.get('useful',0),
            'funny': review.get('funny',0),
            'cool': review.get('cool',0)
        }
    }

    if isinstance(text,(str,int,float,bool,type(None))):
        result['original_text'] = text
    else:
        result['original_text']= str(text) if text else None

    if text is None :
        result['error_type'] = 'missing_text'
        result['action_taken'] = 'filled_with_placeholder'
        result ['healed_text'] = 'No review text provided.'
        result['was_healed'] = True
        return result
    elif not isinstance(text,str):
        result['error_type'] = 'wrong_type'
        try :
            converted_text = str(text).strip()
            result['healed_text'] = converted_text if converted_text else 'No review text provided.'
        except Exception as e:
            result['healed_text'] = 'Conversion to string failed. No review text provided.'
        
        result['action_taken'] = 'type_conversion'
        result['was_healed'] = True

    elif not text.strip():
        result['error_type'] = 'empty_text'
        result['healed_text'] = 'No review text provided.'
        result['action_taken'] = 'filled_with_placeholder'
        result['was_healed'] = True
    
    elif not re.search(r'[a-zA-Z0-9]',text):
        result['error_type'] = 'special_characters_only'
        result['healed_text'] = '[non textual content]'
        result['action_taken'] = 'replaced_special_character'
        result['was_healed'] = True

    elif len(text) > Config.MAX_TEXT_LENGTH:
        result['error_type'] = 'too_long'
        result['healed_text'] = text[:Config.MAX_TEXT_LENGTH-3] + '...'
        result['action_taken'] = 'truncated_text'
        result['was_healed'] = True
    else :
        result['healed_text'] = text.strip()
        result['was_healed'] = False

    return result


def _analyze_with_ollama(healed_reviews: list[dict], model_info: dict) -> list[dict]:
    import ollama
    import time

    model_name = model_info.get('model_name')
    ollama_host = model_info.get('ollama_host', Config.OLLAMA_HOST)

    try :
        client = ollama.Client(host=ollama_host)
    except Exception as e:
        logger.error(f"Error initializing Ollama client: {e}")
        return _created_degraded_results(healed_reviews,str(e))
    
    results = []
    total =len(healed_reviews)
    
    for idx, review in enumerate(healed_reviews):
        text = review.get('healed_text','')
        prediction = None

        for attempt in range(Config.OLLAMA_RETRIES):
            try :
                prompt =(
                    f"""
                    Analyze the sentiment of this review as POSITIVE, NEGATIVE, or NEUTRAL. 
                    Review : "{text}"
                    Reply ONLY with a JSON object in the format: {{"sentiment": "POSITIVE", "confidence": 0.95}}.
                    """
                )

                response = client.chat(
                    model = model_name,
                    messages=[{"role": "user", "content": prompt}],
                    options={'temperature':0.1}
                )

                response_text = response['message']['content'].strip()
                prediction = _parse_ollama_response(response_text)
                break

            except Exception as e:
                if attempt < Config.OLLAMA_RETRIES -1:
                    logger.warning(f"Attempt {attempt +1} failed for review {review.get('review_id')}: {e}. Retrying...")
                    time.sleep(1)
                else:
                    logger.error(f"All attempts failed for review {review.get('review_id')}: {e}.")
                    prediction = {'label':'NEUTRAL','score':0.5,'error': str(e)}
        
        if (idx+1)%10 ==0 or (idx+1) == total:
            logger.info(f"Processed {idx+1}/{total} reviews for sentiment analysis.")

        results.append({
            'review_id': review.get('review_id'),
            'business_id': review.get('business_id'),
            'stars': review.get('stars',0),
            'text': review.get('healed_text',''),
            'original_text': review.get('original_text',''),
            'predicted_sentiment': prediction.get('label'),
            'confidence_score': round( prediction.get('score'),4),
            'status': 'healed' if review.get('was_healed') else 'sucess',
            'healing_applied': review.get('was_healed'),
            'healing_action': review.get('action_taken') if review.get('was_healed') else None,
            'error_type': review.get('error_type') if review.get('was_healed') else None,
            'metadata': review.get('metadata',{})
        })
    
    logger.info(f"Completed sentiment analysis for {len(results)}/{total} reviews.")

    return results

def _created_degraded_results(healed_reviews: list[dict], error_message: str) -> list[dict]:
    return [
        {
            **review,
            'text': review.get('healed_text',''),
            'predicted_sentiment': 'NEUTRAL',
            'confidence_score': 0.5,
            'status': 'degraded',
            'error_message': error_message,
        }
        for review in healed_reviews
    ]




    


@dag(
    dag_id='self_healing_agentic_sentiment_analysis_pipeline',
    default_args=default_args,
    description='Pipeline to perform sentiment analysis using Ollama models',
    schedule=None,
    start_date=datetime(2025, 12, 16),
    catchup=False,
    tags=['agentic','sentiment-analysis','ollama','nlp','yelp_reviews'],

    params= { 
        'input_file': Param(
            default=Config.INPUT_FILE,
            type='string',
            description='Path to the input JSON file containing Yelp reviews.'
        ),
    
        'batch_size': Param(
            default=Config.DEFAULT_BATCH_SIZE,
            type='integer',
            description='Number of reviews to process in each batch.'
        ),
        'offset': Param(
            default=Config.DEFAULT_OFFSET,
            type='integer',
            description='Offset to start reading reviews from the input file.'
        ),

        'ollama_model': Param(
            default=Config.OLLAMA_MODEL,
            type='string',
            description='Name of the Ollama model to use for sentiment analysis.'
        ),


    },
    
    render_template_as_native_obj=True
)

def self_healing_pipeline():
    @task

    def load__model ():
        context = get_current_context()
        params = context['params']
        model_name = params.get('ollama_model', Config.OLLAMA_MODEL)
        logger.info(f"Model name from params: {model_name}")
        return _load_ollama_model(model_name)
        

    @task
    def load_reviews():
        context = get_current_context()
        params = context['params']
        batch_size = params.get('batch_size', Config.DEFAULT_BATCH_SIZE)
        offset = params.get('offset', Config.DEFAULT_OFFSET)
        return _load_from_file(params, batch_size, offset)

    load_ollama_model = load__model()
    reviews = load_reviews()

    @task
    def diagnose_and_heal_batch (reviews :list[dict]):
        healed_reviews = [_heal_review(review) for review in reviews]
        healed_count = sum(1 for r in healed_reviews if r.get('was_healed', True))
        logger.info(f"Healed {healed_count} out of {len(reviews)} reviews in the batch.")
        return healed_reviews
    
    @task
    def batch_analyze_sentiments(healed_reviews: list[dict], model_info: dict):
        if not healed_reviews:
            return []
        logger.info(f"Analyzing sentiments for {len(healed_reviews)} healed reviews.")
        return _analyze_with_ollama(healed_reviews, model_info)
    
    @task
    def aggregate_results(results: list[dict]):
        """Aggregate results and save to output file"""
        context = get_current_context()
        params = context['params']
        
        # Ensure results is a flat list
        if not results:
            logger.warning("No results to aggregate")
            return {
                'totals': {'processed': 0, 'successful': 0, 'healed': 0, 'degraded': 0},
                'rates': {'success_rate': 0, 'healing_rate': 0, 'degraded_rate': 0}
            }

        total = len(results)

        success_count = sum(1 for r in results if r.get('status') == 'sucess')
        healed_count = sum(1 for r in results if r.get('status')== 'healed')
        degraded_count = sum(1 for r in results if r.get('status') == 'degraded')

        sentiment_dis = {'POSITIVE':0,'NEGATIVE':0,'NEUTRAL':0}
        for r in results:
            sentiment = r.get('predicted_sentiment','NEUTRAL')
            sentiment_dis[sentiment] = sentiment_dis.get(sentiment,0) +1
        
        healing_stats = {}
        for r in results:
            if r.get('healing_applied'):
                action = r.get('healing_action','unknown')
                healing_stats[action] = healing_stats.get(action,0) +1

        star_sentiment ={}

        for r in results:
            stars = r.get('stars',0)
            sentiment = r.get('predicted_sentiment')
            if stars and sentiment:
                key = f'{int(stars)}_star'
                star_sentiment[stars] = {'POSITIVE':0,'NEGATIVE':0,'NEUTRAL':0}

            if sentiment in star_sentiment[stars]:
                star_sentiment[stars][sentiment] +=1
        
        confidence_by_status = {'sucess':[],'healed':[],'degraded':[]}
        for r in results:
            status = r.get('status')
            confidence = r.get('confidence_score',0.0)
            if status in confidence_by_status:
                confidence_by_status[status].append(confidence)
        
        avg_confidence = {
            status: round(sum(confidences)/len(confidences),4) if confidences else 0.0
            for status, confidences in confidence_by_status.items()
        }

        summary = {
            'run_info':{
                'timestamp': datetime.utcnow().isoformat(),
                'batch_size': params.get('batch_size', Config.DEFAULT_BATCH_SIZE),
                'offset': params.get('offset', Config.DEFAULT_OFFSET),
                'input_file': params.get('input_file', Config.INPUT_FILE),
            },
            'totals':
            {
                'processed': total,
                'successful': success_count,
                'healed': healed_count,
                'degraded': degraded_count
            },

            'rates':{
                'success_rate': round(success_count/max(total,1),4) ,
                'healing_rate': round(healed_count/max(total,1),4) ,
                'degraded_rate': round(degraded_count/max(total,1),4) 
        },

        'sentiment_distribution': sentiment_dis,
        'healing_statistics': healing_stats,
        'star_sentiment_correlation': star_sentiment,
        'average_confidence_scores': avg_confidence,
        'results': results
}
        os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
        timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        offset = params.get('offset', Config.DEFAULT_OFFSET)
        output_file = f'{Config.OUTPUT_DIR}/sentiment_analysis_results_offset_{timestamp}_{offset}.json'

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2,default=str,ensure_ascii=False)
        
        logger.info("=" * 80)
        logger.info(f"BATCH PROCESSING COMPLETE - Summary written to {output_file}")
        logger.info(f"Processed {total} reviews: {success_count} successful, {healed_count} healed, {degraded_count} degraded")
        logger.info(f"Success Rate: {summary['rates']['success_rate']:.2%}, Healing Rate: {summary['rates']['healing_rate']:.2%}")
        logger.info("=" * 80)

        return {k:v for k,v in summary.items() if k != 'results'}
    
    @task
    def generate_health_report(summary: dict):
        
        total = summary['totals']['processed']
        success = summary['totals']['successful']
        healed = summary['totals']['healed']
        degraded = summary['totals']['degraded']

        if degraded > total*0.1:
            health_status = 'CRITICAL'
        elif degraded>0 :
            health_status = 'DEGRADED'
        elif healed > total*0.5:
            health_status = 'WARNING'
        else:
            health_status = 'HEALTHY'
        
        report =  {
            'pipeline': 'self_healing_agentic_sentiment_analysis_pipeline',
            'timestamp': datetime.utcnow().isoformat(),
            'health_status': health_status,
            'run_info': summary['run_info'],
            'metrics': {
                'total_processed': total,
                'success_rate': summary['rates']['success_rate'],
                'healing_rate': summary['rates']['healing_rate'],
            },

            'sentiment_distribution': summary['sentiment_distribution'],
            'healing_statistics': summary['healing_statistics'],
            'average_confidence_scores': summary['average_confidence_scores']}
        
        logger.info(f"Pipeline Health Report: {json.dumps(report, indent=2)}")
        logger.info(f"success_rate: {summary['rates']['success_rate']}, healing_rate: {summary['rates']['healing_rate']}, degraded_rate: {summary['rates']['degraded_rate']}")
        return report
    
    healed_reviews = diagnose_and_heal_batch(reviews)
    analyzed_results = batch_analyze_sentiments(healed_reviews, load_ollama_model)
    summary = aggregate_results(analyzed_results)
    health_report = generate_health_report(summary)

self_healing_pipeline_dag = self_healing_pipeline()