import os
import pickle
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, IsolationForest, VotingClassifier
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix
from sklearn.naive_bayes import MultinomialNB
from sklearn.svm import SVC
from sklearn.neural_network import MLPClassifier
from sklearn.decomposition import PCA
from sklearn.cluster import DBSCAN
import joblib
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
import warnings
warnings.filterwarnings('ignore')

from database import get_db_connection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EmailRiskClassifier:
    """Advanced ML classifier for email risk assessment"""

    def __init__(self, models_dir='ml_models'):
        self.models_dir = models_dir
        os.makedirs(models_dir, exist_ok=True)

        # Model paths
        self.ensemble_model_path = os.path.join(models_dir, 'ensemble_risk_classifier.pkl')
        self.vectorizer_path = os.path.join(models_dir, 'text_vectorizer.pkl')
        self.scaler_path = os.path.join(models_dir, 'feature_scaler.pkl')
        self.label_encoder_path = os.path.join(models_dir, 'label_encoder.pkl')
        self.anomaly_detector_path = os.path.join(models_dir, 'anomaly_detector.pkl')

        # Models
        self.ensemble_model = None
        self.text_vectorizer = None
        self.feature_scaler = None
        self.label_encoder = None
        self.anomaly_detector = None

        # Feature names for interpretation
        self.feature_names = []

    def extract_features(self, emails_df: pd.DataFrame) -> Tuple[np.ndarray, List[str]]:
        """Extract comprehensive features from email data"""
        features = []
        feature_names = []

        # Text features using TF-IDF
        text_data = (emails_df['subject'].fillna('') + ' ' + 
                    emails_df['justifications'].fillna('') + ' ' +
                    emails_df['policy_name'].fillna('')).values

        if self.text_vectorizer is None:
            self.text_vectorizer = TfidfVectorizer(
                max_features=1000,
                stop_words='english',
                ngram_range=(1, 2),
                min_df=2
            )
            text_features = self.text_vectorizer.fit_transform(text_data).toarray()
        else:
            text_features = self.text_vectorizer.transform(text_data).toarray()

        features.append(text_features)
        feature_names.extend([f'text_{i}' for i in range(text_features.shape[1])])

        # Numerical features
        numerical_features = []

        # Sender domain analysis
        sender_domains = emails_df['sender'].fillna('').apply(
            lambda x: x.split('@')[-1] if '@' in x else 'unknown'
        )
        external_sender = (sender_domains != 'company.com').astype(int)
        numerical_features.append(external_sender.values.reshape(-1, 1))
        feature_names.append('external_sender')

        # Attachment analysis
        has_attachments = emails_df['attachments'].fillna('').apply(
            lambda x: 1 if x and x != '-' else 0
        ).values.reshape(-1, 1)
        numerical_features.append(has_attachments)
        feature_names.append('has_attachments')

        # Risky attachment extensions
        risky_extensions = ['.exe', '.zip', '.rar', '.bat', '.scr']
        risky_attachments = emails_df['attachments'].fillna('').apply(
            lambda x: 1 if any(ext in x.lower() for ext in risky_extensions) else 0
        ).values.reshape(-1, 1)
        numerical_features.append(risky_attachments)
        feature_names.append('risky_attachments')

        # Time-based features
        emails_df['_time'] = pd.to_datetime(emails_df['_time'], errors='coerce')
        hour_of_day = emails_df['_time'].dt.hour.fillna(12).values.reshape(-1, 1)
        day_of_week = emails_df['_time'].dt.dayofweek.fillna(2).values.reshape(-1, 1)
        numerical_features.extend([hour_of_day, day_of_week])
        feature_names.extend(['hour_of_day', 'day_of_week'])

        # Policy count
        policy_count = emails_df['policy_name'].fillna('').apply(
            lambda x: len(x.split(',')) if x else 0
        ).values.reshape(-1, 1)
        numerical_features.append(policy_count)
        feature_names.append('policy_count')

        # Department encoding
        dept_encoded = LabelEncoder().fit_transform(
            emails_df['department'].fillna('unknown')
        ).reshape(-1, 1)
        numerical_features.append(dept_encoded)
        feature_names.append('department_encoded')

        # Recipient analysis
        recipient_count = emails_df['recipients'].fillna('').apply(
            lambda x: len(x.split(',')) if x else 0
        ).values.reshape(-1, 1)
        numerical_features.append(recipient_count)
        feature_names.append('recipient_count')

        # External recipients
        external_recipients = emails_df['recipients'].fillna('').apply(
            lambda x: 1 if x and '@' in x and 'company.com' not in x else 0
        ).values.reshape(-1, 1)
        numerical_features.append(external_recipients)
        feature_names.append('external_recipients')

        # Combine numerical features
        if numerical_features:
            numerical_array = np.hstack(numerical_features)
            features.append(numerical_array)

        # Combine all features
        final_features = np.hstack(features)

        # Scale numerical features
        if self.feature_scaler is None:
            self.feature_scaler = StandardScaler()
            # Only scale the numerical part (not text features)
            text_size = text_features.shape[1]
            numerical_part = final_features[:, text_size:]
            scaled_numerical = self.feature_scaler.fit_transform(numerical_part)
            final_features = np.hstack([final_features[:, :text_size], scaled_numerical])
        else:
            text_size = text_features.shape[1]
            numerical_part = final_features[:, text_size:]
            scaled_numerical = self.feature_scaler.transform(numerical_part)
            final_features = np.hstack([final_features[:, :text_size], scaled_numerical])

        self.feature_names = feature_names
        return final_features, feature_names

    def prepare_risk_labels(self, emails_df: pd.DataFrame) -> np.ndarray:
        """Convert final outcomes to risk levels"""
        risk_mapping = {
            'Escalated': 'critical',
            'Flagged': 'high',
            'Under Review': 'medium',
            'Pending Review': 'medium',
            'Cleared': 'low',
            'Approved': 'low'
        }

        risk_labels = emails_df['final_outcome'].fillna('medium').map(
            lambda x: risk_mapping.get(x, 'medium')
        )

        if self.label_encoder is None:
            self.label_encoder = LabelEncoder()
            encoded_labels = self.label_encoder.fit_transform(risk_labels)
        else:
            encoded_labels = self.label_encoder.transform(risk_labels)

        return encoded_labels

    def create_ensemble_model(self) -> VotingClassifier:
        """Create an ensemble model with multiple classifiers"""

        # Base classifiers
        rf_classifier = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            class_weight='balanced'
        )

        nb_classifier = MultinomialNB(alpha=0.1)

        svm_classifier = SVC(
            probability=True,
            kernel='rbf',
            random_state=42,
            class_weight='balanced'
        )

        mlp_classifier = MLPClassifier(
            hidden_layer_sizes=(100, 50),
            max_iter=500,
            random_state=42,
            early_stopping=True
        )

        # Create ensemble
        ensemble = VotingClassifier(
            estimators=[
                ('rf', rf_classifier),
                ('nb', nb_classifier),
                ('svm', svm_classifier),
                ('mlp', mlp_classifier)
            ],
            voting='soft'
        )

        return ensemble

    def train_models(self) -> Dict[str, float]:
        """Train all ML models"""
        logger.info("Starting model training...")

        try:
            # Load training data
            conn = get_db_connection()
            query = """
                SELECT * FROM emails 
                WHERE final_outcome IS NOT NULL 
                AND final_outcome != ''
                AND (subject IS NOT NULL OR justifications IS NOT NULL)
            """
            emails_df = conn.execute(query).df()
            conn.close()

            if emails_df.empty or len(emails_df) < 20:
                logger.warning("Insufficient training data")
                return {'error': 'Insufficient training data'}

            logger.info(f"Training on {len(emails_df)} email records")

            # Extract features and labels
            X, feature_names = self.extract_features(emails_df)
            y = self.prepare_risk_labels(emails_df)

            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42, stratify=y
            )

            # Train ensemble model
            self.ensemble_model = self.create_ensemble_model()
            self.ensemble_model.fit(X_train, y_train)

            # Evaluate ensemble
            y_pred = self.ensemble_model.predict(X_test)
            ensemble_accuracy = accuracy_score(y_test, y_pred)

            # Train anomaly detector
            self.anomaly_detector = IsolationForest(
                contamination=0.1,
                random_state=42
            )
            self.anomaly_detector.fit(X_train)

            # Save models
            self.save_models()

            # Get feature importance
            feature_importance = self.get_feature_importance()

            results = {
                'ensemble_accuracy': ensemble_accuracy,
                'training_samples': len(emails_df),
                'test_samples': len(X_test),
                'feature_count': X.shape[1],
                'top_features': feature_importance[:10]
            }

            logger.info(f"Model training completed. Accuracy: {ensemble_accuracy:.3f}")
            return results

        except Exception as e:
            logger.error(f"Model training error: {e}")
            return {'error': str(e)}

    def get_feature_importance(self) -> List[Tuple[str, float]]:
        """Get feature importance from random forest in ensemble"""
        if self.ensemble_model is None:
            return []

        try:
            # Get feature importance from random forest
            rf_model = None
            for name, model in self.ensemble_model.named_estimators_.items():
                if name == 'rf':
                    rf_model = model
                    break

            if rf_model and hasattr(rf_model, 'feature_importances_'):
                importance_pairs = list(zip(self.feature_names, rf_model.feature_importances_))
                importance_pairs.sort(key=lambda x: x[1], reverse=True)
                return importance_pairs

        except Exception as e:
            logger.error(f"Error getting feature importance: {e}")

        return []

    def predict_risk(self, email_data: Dict[str, Any]) -> Dict[str, Any]:
        """Predict risk level for a single email"""
        try:
            if self.ensemble_model is None:
                self.load_models()

            if self.ensemble_model is None:
                return {'error': 'No trained model available'}

            # Convert single email to dataframe
            email_df = pd.DataFrame([email_data])

            # Extract features
            X, _ = self.extract_features(email_df)

            # Make predictions
            risk_pred = self.ensemble_model.predict(X)[0]
            risk_proba = self.ensemble_model.predict_proba(X)[0]

            # Anomaly detection
            anomaly_score = self.anomaly_detector.decision_function(X)[0]
            is_anomaly = self.anomaly_detector.predict(X)[0] == -1

            # Convert prediction back to risk level
            risk_level = self.label_encoder.inverse_transform([risk_pred])[0]

            # Get class probabilities
            risk_probabilities = {}
            for i, class_name in enumerate(self.label_encoder.classes_):
                risk_probabilities[class_name] = float(risk_proba[i])

            # Feature analysis
            feature_analysis = self.analyze_features(X[0])

            return {
                'risk_level': risk_level,
                'confidence': float(max(risk_proba)),
                'risk_probabilities': risk_probabilities,
                'anomaly_score': float(anomaly_score),
                'is_anomaly': bool(is_anomaly),
                'feature_analysis': feature_analysis
            }

        except Exception as e:
            logger.error(f"Risk prediction error: {e}")
            return {'error': str(e)}

    def analyze_features(self, feature_vector: np.ndarray) -> Dict[str, Any]:
        """Analyze individual features for interpretation"""
        analysis = {}

        try:
            # Get feature importance
            importance_pairs = self.get_feature_importance()

            # Analyze top contributing features
            top_features = []
            for i, (feature_name, importance) in enumerate(importance_pairs[:5]):
                if i < len(feature_vector):
                    feature_value = feature_vector[i]
                    top_features.append({
                        'name': feature_name,
                        'value': float(feature_value),
                        'importance': float(importance)
                    })

            analysis['top_contributing_features'] = top_features
            analysis['total_features'] = len(feature_vector)

        except Exception as e:
            logger.error(f"Feature analysis error: {e}")
            analysis['error'] = str(e)

        return analysis

    def batch_predict(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Predict risks for a batch of emails"""
        try:
            conn = get_db_connection()

            # Get emails without ML predictions
            emails_df = conn.execute("""
                SELECT * FROM emails 
                WHERE final_outcome IS NULL 
                OR final_outcome = 'Pending'
                ORDER BY _time DESC 
                LIMIT ?
            """, [limit]).df()

            conn.close()

            if emails_df.empty:
                return []

            results = []
            for _, email_row in emails_df.iterrows():
                email_dict = email_row.to_dict()
                prediction = self.predict_risk(email_dict)
                prediction['email_id'] = email_dict['id']
                results.append(prediction)

            return results

        except Exception as e:
            logger.error(f"Batch prediction error: {e}")
            return []

    def save_models(self):
        """Save all trained models"""
        try:
            if self.ensemble_model:
                joblib.dump(self.ensemble_model, self.ensemble_model_path)
            if self.text_vectorizer:
                joblib.dump(self.text_vectorizer, self.vectorizer_path)
            if self.feature_scaler:
                joblib.dump(self.feature_scaler, self.scaler_path)
            if self.label_encoder:
                joblib.dump(self.label_encoder, self.label_encoder_path)
            if self.anomaly_detector:
                joblib.dump(self.anomaly_detector, self.anomaly_detector_path)

            logger.info("Models saved successfully")

        except Exception as e:
            logger.error(f"Error saving models: {e}")

    def load_models(self):
        """Load trained models"""
        try:
            if os.path.exists(self.ensemble_model_path):
                self.ensemble_model = joblib.load(self.ensemble_model_path)
            if os.path.exists(self.vectorizer_path):
                self.text_vectorizer = joblib.load(self.vectorizer_path)
            if os.path.exists(self.scaler_path):
                self.feature_scaler = joblib.load(self.scaler_path)
            if os.path.exists(self.label_encoder_path):
                self.label_encoder = joblib.load(self.label_encoder_path)
            if os.path.exists(self.anomaly_detector_path):
                self.anomaly_detector = joblib.load(self.anomaly_detector_path)

            logger.info("Models loaded successfully")

        except Exception as e:
            logger.error(f"Error loading models: {e}")


class AdvancedAnalytics:
    """Advanced analytics for correlation and pattern detection"""

    def __init__(self):
        self.correlation_threshold = 0.5

    def analyze_correlations(self) -> Dict[str, Any]:
        """Analyze correlations between email features and outcomes"""
        try:
            conn = get_db_connection()

            # Get comprehensive dataset
            query = """
                SELECT sender, department, bunit, final_outcome, 
                       CASE WHEN attachments IS NOT NULL AND attachments != '-' THEN 1 ELSE 0 END as has_attachments,
                       CASE WHEN leaver = 'Yes' THEN 1 ELSE 0 END as is_leaver,
                       CASE WHEN sender LIKE '%@company.com' THEN 0 ELSE 1 END as external_sender
                FROM emails 
                WHERE final_outcome IS NOT NULL
            """

            df = conn.execute(query).df()
            conn.close()

            if df.empty:
                return {'error': 'No data available for correlation analysis'}

            # Create outcome numeric mapping for correlation analysis
            outcome_mapping = {
                'escalated': 3, 'high_risk': 3, 'critical': 3,
                'medium_risk': 2, 'warning': 2, 'pending_review': 2,
                'cleared': 1, 'approved': 1, 'resolved': 1,
                'excluded': 0, 'whitelisted': 0
            }

            # Add outcome_numeric column safely
            if 'final_outcome' in df.columns:
                df['outcome_numeric'] = df['final_outcome'].map(outcome_mapping).fillna(1)
            else:
                df['outcome_numeric'] = 1

            report = {}
            # Correlation analysis
            try:
                correlations = {}

                # Check if we have the necessary columns
                if 'outcome_numeric' in df.columns and not df.empty:
                    # High-risk sender correlation
                    high_risk_senders = df[df['outcome_numeric'] >= 2]['sender'].value_counts()
                    correlations['high_risk_senders'] = high_risk_senders.head(10).to_dict() if not high_risk_senders.empty else {}

                    # Department outcome correlation
                    if 'department' in df.columns and not df['department'].isna().all():
                        dept_outcome_corr = df.groupby('department')['outcome_numeric'].mean().sort_values(ascending=False)
                        correlations['department_outcome'] = dept_outcome_corr.head(10).to_dict() if not dept_outcome_corr.empty else {}

                report['correlations'] = correlations
            except Exception as e:
                logging.error(f"Correlation analysis error: {e}")
                report['correlations'] = {}

            return report

        except Exception as e:
            logger.error(f"Correlation analysis error: {e}")
            return {'error': str(e)}

    def detect_anomalies(self) -> List[Dict[str, Any]]:
        """Detect anomalous email patterns"""
        try:
            conn = get_db_connection()

            # Get recent emails for anomaly detection
            anomaly_query = """
                SELECT sender, COUNT(*) as email_count,
                       AVG(CASE WHEN final_outcome = 'escalated' THEN 1.0 ELSE 0.0 END) as escalation_rate
                FROM emails 
                WHERE _time >= CURRENT_DATE - INTERVAL 30 DAY
                GROUP BY sender
                HAVING COUNT(*) > 5
                ORDER BY escalation_rate DESC, email_count DESC
                LIMIT 20
            """

            df_sender_anomalies = conn.execute(anomaly_query).df()

            # Unusual department activity
            dept_query = """
                SELECT department, COUNT(*) as department_count
                FROM emails
                WHERE _time >= CURRENT_DATE - INTERVAL 30 DAY
                GROUP BY department
                ORDER BY department_count DESC
                LIMIT 20
            """
            df_dept_anomalies = conn.execute(dept_query).df()

            # Time-based anomalies
            time_query = """
                SELECT CAST(strftime('%H', _time) AS INTEGER) as hour, COUNT(*) as hour_count
                FROM emails
                WHERE _time >= CURRENT_DATE - INTERVAL 30 DAY
                GROUP BY hour
                ORDER BY hour_count DESC
                LIMIT 20
            """
            df_time_anomalies = conn.execute(time_query).df()

            conn.close()

            anomalies = []

            # Process sender anomalies
            if not df_sender_anomalies.empty:
                sender_counts_quantile = df_sender_anomalies['email_count'].quantile(0.95)
                for _, row in df_sender_anomalies.iterrows():
                    if row['email_count'] > sender_counts_quantile:
                        anomalies.append({
                            'type': 'high_frequency_sender',
                            'sender': row['sender'],
                            'count': int(row['email_count']),
                            'threshold': float(sender_counts_quantile)
                        })

            # Process department anomalies
            if not df_dept_anomalies.empty:
                dept_counts_quantile = df_dept_anomalies['department_count'].quantile(0.9)
                for _, row in df_dept_anomalies.iterrows():
                    if row['department_count'] > dept_counts_quantile:
                        anomalies.append({
                            'type': 'high_activity_department',
                            'department': row['department'],
                            'count': int(row['department_count'])
                        })

            # Process time anomalies
            if not df_time_anomalies.empty:
                hour_counts_quantile = df_time_anomalies['hour_count'].quantile(0.9)
                hour_counts_lower_quantile = df_time_anomalies['hour_count'].quantile(0.1)
                for _, row in df_time_anomalies.iterrows():
                    if row['hour_count'] > hour_counts_quantile or row['hour_count'] < hour_counts_lower_quantile:
                        anomalies.append({
                            'type': 'unusual_time_activity',
                            'hour': int(row['hour']),
                            'count': int(row['hour_count'])
                        })

            return anomalies[:20]  # Limit results

        except Exception as e:
            logger.error(f"Anomaly detection error: {e}")
            return []


# Global classifier instance
email_classifier = EmailRiskClassifier()
analytics_engine = AdvancedAnalytics()

def train_advanced_models() -> Dict[str, Any]:
    """Train all advanced ML models"""
    return email_classifier.train_models()

def predict_email_risk(email_data: Dict[str, Any]) -> Dict[str, Any]:
    """Predict risk for a single email"""
    return email_classifier.predict_risk(email_data)

def get_ml_insights(email_id: int) -> Dict[str, Any]:
    """Get comprehensive ML insights for an email"""
    try:
        conn = get_db_connection()
        email_data = conn.execute("SELECT * FROM emails WHERE id = ?", [email_id]).fetchone()
        conn.close()

        if not email_data:
            return {'error': 'Email not found'}

        # Convert to dict
        columns = ['id', '_time', 'sender', 'subject', 'attachments', 'recipients',
                  'time_month', 'leaver', 'termination_date', 'bunit', 'department',
                  'user_response', 'final_outcome', 'policy_name', 'justifications']
        email_dict = dict(zip(columns, email_data))

        # Get ML prediction
        prediction = predict_email_risk(email_dict)

        # Add additional insights
        insights = {
            'ml_prediction': prediction,
            'email_metadata': {
                'id': email_dict['id'],
                'sender': email_dict['sender'],
                'subject': email_dict['subject'],
                'department': email_dict['department']
            }
        }

        return insights

    except Exception as e:
        logger.error(f"ML insights error: {e}")
        return {'error': str(e)}

def get_analytics_report() -> Dict[str, Any]:
    """Generate comprehensive analytics report"""
    try:
        correlations = analytics_engine.analyze_correlations()
        anomalies = analytics_engine.detect_anomalies()

        return {
            'correlations': correlations,
            'anomalies': anomalies,
            'generated_at': datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Analytics report error: {e}")
        return {'error': str(e)}

if __name__ == "__main__":
    # Command line interface for model training
    import argparse

    parser = argparse.ArgumentParser(description='Train Email Guardian ML models')
    parser.add_argument('--train', action='store_true', help='Train models')
    parser.add_argument('--predict', type=int, help='Predict risk for email ID')
    parser.add_argument('--analytics', action='store_true', help='Generate analytics report')

    args = parser.parse_args()

    if args.train:
        print("Training models...")
        results = train_advanced_models()
        print(f"Training results: {results}")

    elif args.predict:
        print(f"Predicting risk for email {args.predict}...")
        insights = get_ml_insights(args.predict)
        print(f"Insights: {insights}")

    elif args.analytics:
        print("Generating analytics report...")
        report = get_analytics_report()
        print(f"Report: {report}")

    else:
        print("Use --train, --predict <email_id>, or --analytics")