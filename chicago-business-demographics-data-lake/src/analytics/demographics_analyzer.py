"""
Demographics Analysis Engine for Chicago Business Owners
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging
from collections import Counter
import re

logger = logging.getLogger(__name__)

class DemographicsAnalyzer:
    """Analyzes business owner demographics and patterns"""
    
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.individual_owners = df[df['Is Individual Owner'] == True].copy()
        self.corporate_owners = df[df['Is Individual Owner'] == False].copy()
    
    def analyze_ownership_patterns(self) -> Dict:
        """Analyze business ownership patterns"""
        logger.info("Analyzing ownership patterns...")
        
        patterns = {
            'total_businesses': self.df['Account Number'].nunique(),
            'total_owners': len(self.df),
            'individual_owners': len(self.individual_owners),
            'corporate_owners': len(self.corporate_owners),
            'avg_owners_per_business': len(self.df) / self.df['Account Number'].nunique(),
            'businesses_with_multiple_owners': self.df.groupby('Account Number').size().gt(1).sum(),
            'single_owner_businesses': self.df.groupby('Account Number').size().eq(1).sum()
        }
        
        # Ownership distribution
        ownership_counts = self.df.groupby('Account Number').size()
        patterns['ownership_distribution'] = {
            '1_owner': (ownership_counts == 1).sum(),
            '2_owners': (ownership_counts == 2).sum(),
            '3_owners': (ownership_counts == 3).sum(),
            '4_owners': (ownership_counts == 4).sum(),
            '5_or_more_owners': (ownership_counts >= 5).sum()
        }
        
        return patterns
    
    def analyze_name_demographics(self) -> Dict:
        """Analyze name-based demographics"""
        logger.info("Analyzing name demographics...")
        
        # Extract first names for analysis
        first_names = self.individual_owners['Owner First Name'].dropna()
        
        demographics = {
            'total_individual_owners': len(first_names),
            'unique_first_names': first_names.nunique(),
            'most_common_first_names': first_names.value_counts().head(20).to_dict(),
            'name_length_stats': {
                'avg_length': first_names.str.len().mean(),
                'min_length': first_names.str.len().min(),
                'max_length': first_names.str.len().max()
            }
        }
        
        # Analyze name patterns
        demographics['name_patterns'] = self._analyze_name_patterns(first_names)
        
        return demographics
    
    def _analyze_name_patterns(self, names: pd.Series) -> Dict:
        """Analyze patterns in names"""
        patterns = {
            'single_letter_names': (names.str.len() == 1).sum(),
            'two_letter_names': (names.str.len() == 2).sum(),
            'names_with_numbers': names.str.contains(r'\d', na=False).sum(),
            'names_with_special_chars': names.str.contains(r'[^a-zA-Z\s]', na=False).sum(),
            'names_with_spaces': names.str.contains(r'\s', na=False).sum()
        }
        
        return patterns
    
    def analyze_business_roles(self) -> Dict:
        """Analyze business roles and titles"""
        logger.info("Analyzing business roles...")
        
        roles = {
            'total_roles': len(self.df),
            'unique_roles': self.df['Title'].nunique(),
            'role_distribution': self.df['Title'].value_counts().to_dict(),
            'most_common_roles': self.df['Title'].value_counts().head(10).to_dict(),
            'role_by_ownership_type': {
                'individual_roles': self.individual_owners['Title'].value_counts().to_dict(),
                'corporate_roles': self.corporate_owners['Title'].value_counts().to_dict()
            }
        }
        
        # Analyze role patterns
        roles['leadership_roles'] = self._identify_leadership_roles()
        roles['ownership_roles'] = self._identify_ownership_roles()
        
        return roles
    
    def _identify_leadership_roles(self) -> Dict:
        """Identify leadership roles"""
        leadership_keywords = ['CEO', 'PRESIDENT', 'MANAGING', 'DIRECTOR', 'CHAIRMAN', 'FOUNDER']
        
        leadership_roles = {}
        for keyword in leadership_keywords:
            matching_roles = self.df[self.df['Title'].str.contains(keyword, case=False, na=False)]
            leadership_roles[keyword.lower()] = len(matching_roles)
        
        return leadership_roles
    
    def _identify_ownership_roles(self) -> Dict:
        """Identify ownership-related roles"""
        ownership_keywords = ['OWNER', 'MEMBER', 'SHAREHOLDER', 'PARTNER']
        
        ownership_roles = {}
        for keyword in ownership_keywords:
            matching_roles = self.df[self.df['Title'].str.contains(keyword, case=False, na=False)]
            ownership_roles[keyword.lower()] = len(matching_roles)
        
        return ownership_roles
    
    def analyze_business_names(self) -> Dict:
        """Analyze business legal names"""
        logger.info("Analyzing business names...")
        
        business_names = self.df['Legal Name'].dropna()
        
        name_analysis = {
            'total_businesses': len(business_names),
            'unique_business_names': business_names.nunique(),
            'name_length_stats': {
                'avg_length': business_names.str.len().mean(),
                'min_length': business_names.str.len().min(),
                'max_length': business_names.str.len().max()
            },
            'business_name_patterns': self._analyze_business_name_patterns(business_names)
        }
        
        # Common business name patterns
        name_analysis['common_suffixes'] = self._extract_business_suffixes(business_names)
        name_analysis['common_words'] = self._extract_common_words(business_names)
        
        return name_analysis
    
    def _analyze_business_name_patterns(self, names: pd.Series) -> Dict:
        """Analyze patterns in business names"""
        patterns = {
            'llc_count': names.str.contains(r'\bLLC\b', case=False, na=False).sum(),
            'inc_count': names.str.contains(r'\bINC\b', case=False, na=False).sum(),
            'corp_count': names.str.contains(r'\bCORP\b', case=False, na=False).sum(),
            'ltd_count': names.str.contains(r'\bLTD\b', case=False, na=False).sum(),
            'names_with_numbers': names.str.contains(r'\d', na=False).sum(),
            'names_with_special_chars': names.str.contains(r'[^a-zA-Z0-9\s]', na=False).sum()
        }
        
        return patterns
    
    def _extract_business_suffixes(self, names: pd.Series) -> Dict:
        """Extract common business suffixes"""
        suffixes = []
        for name in names:
            # Extract last word as potential suffix
            words = name.split()
            if words:
                suffixes.append(words[-1])
        
        suffix_counts = Counter(suffixes)
        return dict(suffix_counts.most_common(20))
    
    def _extract_common_words(self, names: pd.Series) -> Dict:
        """Extract common words in business names"""
        all_words = []
        for name in names:
            # Split by common delimiters and filter out short words
            words = re.findall(r'\b[a-zA-Z]{3,}\b', name.upper())
            all_words.extend(words)
        
        word_counts = Counter(all_words)
        return dict(word_counts.most_common(50))
    
    def analyze_geographic_patterns(self) -> Dict:
        """Analyze potential geographic patterns in names"""
        logger.info("Analyzing geographic patterns...")
        
        # This is a simplified analysis - in a real scenario, you'd use geocoding
        # or external data sources for more accurate geographic analysis
        
        last_names = self.individual_owners['Owner Last Name'].dropna()
        
        geographic_analysis = {
            'total_last_names': len(last_names),
            'unique_last_names': last_names.nunique(),
            'most_common_last_names': last_names.value_counts().head(20).to_dict(),
            'name_diversity': {
                'shannon_entropy': self._calculate_shannon_entropy(last_names),
                'gini_simpson': self._calculate_gini_simpson(last_names)
            }
        }
        
        return geographic_analysis
    
    def _calculate_shannon_entropy(self, series: pd.Series) -> float:
        """Calculate Shannon entropy for diversity measurement"""
        value_counts = series.value_counts()
        probabilities = value_counts / len(series)
        return -sum(p * np.log2(p) for p in probabilities if p > 0)
    
    def _calculate_gini_simpson(self, series: pd.Series) -> float:
        """Calculate Gini-Simpson diversity index"""
        value_counts = series.value_counts()
        n = len(series)
        return 1 - sum((count / n) ** 2 for count in value_counts)
    
    def generate_comprehensive_report(self) -> Dict:
        """Generate demographics report"""
        logger.info("Generating demographics report...")
        
        report = {
            'metadata': {
                'analysis_timestamp': pd.Timestamp.now().isoformat(),
                'total_records_analyzed': len(self.df),
                'total_businesses': self.df['Account Number'].nunique()
            },
            'ownership_patterns': self.analyze_ownership_patterns(),
            'name_demographics': self.analyze_name_demographics(),
            'business_roles': self.analyze_business_roles(),
            'business_names': self.analyze_business_names(),
            'geographic_patterns': self.analyze_geographic_patterns()
        }
        
        logger.info("Demographics report generated")
        return report
