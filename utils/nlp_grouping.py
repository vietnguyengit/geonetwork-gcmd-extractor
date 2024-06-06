from collections import defaultdict
import spacy
import enchant


class GroupingSimilarTexts:
    def __init__(self):
        # Load SpaCy model
        self.nlp = spacy.load("en_core_web_md")
        # Initialise Australian English spellchecker using pyenchant
        self.spell = enchant.Dict("en_AU")

    def preprocess(self, text):
        """Preprocess a text by correcting typos, tokenizing, and lemmatizing."""
        # Tokenize the text
        doc = self.nlp(text.lower())
        corrected_words = []
        # Correct spelling for each word, only if needed
        for token in doc:
            if self.spell.check(token.text):
                corrected_word = token.text
            else:
                suggestions = self.spell.suggest(token.text)
                corrected_word = suggestions[0] if suggestions else token.text
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
        return list(grouped_values.keys())
