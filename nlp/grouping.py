from collections import defaultdict
import spacy
from spellchecker import SpellChecker


class GroupingSimilarTexts:
    def __init__(self):
        # Load SpaCy model
        self.nlp = spacy.load("en_core_web_md")
        # Initialise spellchecker
        self.spell = SpellChecker()

    def preprocess(self, text):
        """Preprocess a text by correcting typos, tokenizing, and lemmatizing."""
        # Tokenize the text
        doc = self.nlp(text.lower())
        corrected_words = []
        # Correct spelling for each word, only if needed
        for token in doc:
            corrected_word = self.spell.correction(token.text)
            corrected_words.append(corrected_word)

        # Lemmatize the corrected words
        corrected_text = " ".join(corrected_words)
        doc = self.nlp(corrected_text)
        lemmatized_words = [token.lemma_ for token in doc]
        return " ".join(lemmatized_words).upper()

    def group_values(self, values):
        """Group values after preprocessing."""
        grouped_values = defaultdict(list)
        for value in values:
            preprocessed_value = self.preprocess(value)
            grouped_values[preprocessed_value].append(value)  # Retain original casing

        # Optionally, return only the canonical forms
        return list(grouped_values.keys())
