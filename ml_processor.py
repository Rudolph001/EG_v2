import os
import pickle
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
from database import get_db_connection
import logging

MODEL_PATH = 'ml_models/email_classifier.pkl'
VECTORIZER_PATH = 'ml_models/vectorizer.pkl'

def load_model():
    """Load the trained ML model"""
    try:
        if os.path.exists(MODEL_PATH) and os.path.exists(VECTORIZER_PATH):
            with open(MODEL_PATH, 'rb') as f:
                model = pickle.load(f)
            with open(VECTORIZER_PATH, 'rb') as f:
                vectorizer = pickle.load(f)
            return model, vectorizer
        else:
            return None, None
    except Exception as e:
        logging.error(f"Model loading error: {e}")
        return None, None

def save_model(model, vectorizer):
    """Save the trained ML model"""
    try:
        os.makedirs('ml_models', exist_ok=True)
        with open(MODEL_PATH, 'wb') as f:
            pickle.dump(model, f)
        with open(VECTORIZER_PATH, 'wb') as f:
            pickle.dump(vectorizer, f)
        logging.info("Model saved successfully")
    except Exception as e:
        logging.error(f"Model saving error: {e}")

def prepare_training_data():
    """Prepare training data from database"""
    try:
        conn = get_db_connection()
        
        # Get emails with known outcomes for training
        query = """
            SELECT subject, justifications, final_outcome
            FROM emails 
            WHERE final_outcome IS NOT NULL 
            AND subject IS NOT NULL
        """
        
        df = conn.execute(query).df()
        conn.close()
        
        if df.empty:
            logging.warning("No training data available")
            return None, None
        
        # Combine subject and justifications for text features
        df['text'] = df['subject'].fillna('') + ' ' + df['justifications'].fillna('')
        
        # Clean and prepare labels
        df['label'] = df['final_outcome'].fillna('unknown')
        
        return df['text'].values, df['label'].values
        
    except Exception as e:
        logging.error(f"Training data preparation error: {e}")
        return None, None

def train_model():
    """Train the email classification model"""
    try:
        X, y = prepare_training_data()
        
        if X is None or len(X) < 10:
            logging.warning("Insufficient training data")
            return 0.0
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        # Create pipeline with TF-IDF and Naive Bayes
        vectorizer = TfidfVectorizer(max_features=5000, stop_words='english')
        model = MultinomialNB()
        
        # Fit the model
        X_train_vec = vectorizer.fit_transform(X_train)
        model.fit(X_train_vec, y_train)
        
        # Evaluate
        X_test_vec = vectorizer.transform(X_test)
        y_pred = model.predict(X_test_vec)
        accuracy = accuracy_score(y_test, y_pred)
        
        # Save model
        save_model(model, vectorizer)
        
        logging.info(f"Model trained with accuracy: {accuracy:.2%}")
        return accuracy
        
    except Exception as e:
        logging.error(f"Model training error: {e}")
        return 0.0

def classify_email(text):
    """Classify an email using the trained model"""
    try:
        model, vectorizer = load_model()
        
        if model is None or vectorizer is None:
            # Train a model if none exists
            logging.info("No model found, training new model...")
            train_model()
            model, vectorizer = load_model()
            
            if model is None:
                return "unknown"
        
        # Vectorize the text
        text_vec = vectorizer.transform([text])
        
        # Make prediction
        prediction = model.predict(text_vec)[0]
        probability = model.predict_proba(text_vec).max()
        
        logging.info(f"Email classified as: {prediction} (confidence: {probability:.2%})")
        return prediction
        
    except Exception as e:
        logging.error(f"Email classification error: {e}")
        return "unknown"

def get_risk_score(email_data):
    """Calculate risk score for an email
    
    Note: All imported emails are external emails (sent outside the organization),
    so we focus on other risk indicators rather than external recipient status.
    """
    try:
        risk_score = 0
        
        # Check for flagged sender
        conn = get_db_connection()
        flagged = conn.execute(
            "SELECT COUNT(*) FROM flagged_senders WHERE sender = ?", 
            [email_data.get('sender', '')]
        ).fetchone()[0]
        
        if flagged > 0:
            risk_score += 40
        
        # Check if sender is a leaver
        if email_data.get('leaver') == 'Yes' or email_data.get('leaver') == True:
            risk_score += 35
        
        # Check for suspicious attachments
        attachments = email_data.get('attachments', '')
        if attachments and attachments != '-':
            suspicious_extensions = ['.exe', '.zip', '.rar', '.bat', '.scr']
            if any(ext in attachments.lower() for ext in suspicious_extensions):
                risk_score += 30
            else:
                risk_score += 10  # Any attachment adds some risk
        
        # Check recipient patterns (all emails are external, so focus on suspicious patterns)
        recipients = email_data.get('recipients', '')
        if recipients and '@' in recipients:
            # Multiple recipients increase risk (mass distribution)
            recipient_count = len(recipients.split(',')) if recipients else 0
            if recipient_count > 10:
                risk_score += 20  # Many recipients = potential data breach
            elif recipient_count > 5:
                risk_score += 10  # Moderate recipient count
            
            # Check for suspicious recipient domains
            suspicious_domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'protonmail.com']
            if any(domain in recipients.lower() for domain in suspicious_domains):
                risk_score += 15  # Personal email domains are higher risk
        
        # Policy violations
        if email_data.get('policy_name'):
            risk_score += 30
        
        # Department-based risk
        department = email_data.get('department', '').lower()
        high_risk_depts = ['finance', 'legal', 'hr', 'executive']
        if department in high_risk_depts:
            risk_score += 15
        
        # Content analysis
        text = f"{email_data.get('subject', '')} {email_data.get('justifications', '')}"
        if text.strip():
            classification = classify_email(text)
            
            # Modify these classifications and scores to change clearing behavior
            if classification in ['high_risk', 'suspicious', 'escalated']:
                risk_score += 35  # Lower this to be less strict
            elif classification in ['medium_risk', 'warning', 'pending_review']:
                risk_score += 20  # Lower this to be less strict
            elif classification in ['cleared', 'approved', 'safe', 'low_risk']:
                risk_score = max(0, risk_score - 15)  # Increase reduction for more clearing
            
            # Keyword analysis
            high_risk_keywords = ['urgent', 'confidential', 'personal', 'private', 'secret', 'transfer', 'payment', 'invoice']
            text_lower = text.lower()
            keyword_matches = sum(1 for keyword in high_risk_keywords if keyword in text_lower)
            risk_score += keyword_matches * 5
        
        conn.close()
        return min(risk_score, 100)  # Cap at 100
        
    except Exception as e:
        logging.error(f"Risk score calculation error: {e}")
        return 0
