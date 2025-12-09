"""
Filing
~~~

Functions related to reading and writing files.

Authors: Kenneth Schackart
"""

import os
import pickle
import sys
from typing import Any, BinaryIO, Tuple

import pandas as pd
import torch
from transformers import AutoModelForSequenceClassification as classifier
from transformers import AutoModelForTokenClassification as ner_classifier
from transformers import AutoTokenizer
from transformers.tokenization_utils import PreTrainedTokenizer

from .constants import ID2NER_TAG, NER_TAG2ID
from .custom_classes import Metrics


# ---------------------------------------------------------------------------
def get_classif_model(checkpoint_fh: BinaryIO,
                      device: torch.device) -> Tuple[Any, str]:
    """
    Instatiate predictive model from checkpoint

    Supports both new format (weights_only=True, metrics as dicts)
    and old format (weights_only=False, metrics as NamedTuples).

    Params:
    `checkpoint_fh`: Model checkpoint filehandle
    `device`: The `torch.device` to use

    Return:
    Model instance from checkpoint, and model name
    """

    # Try new format first (weights_only=True) - version-resilient
    try:
        checkpoint = torch.load(checkpoint_fh, map_location=device, weights_only=True)
    except (pickle.UnpicklingError, RuntimeError, TypeError) as e:
        # Fall back to old format (weights_only=False) - for legacy checkpoints
        checkpoint_fh.seek(0)  # Reset file pointer
        checkpoint = torch.load(checkpoint_fh, map_location=device, weights_only=False)

    model_name = checkpoint['model_name']
    model = classifier.from_pretrained(model_name, num_labels=2)

    # Remove incompatible keys for newer transformers versions
    state_dict = checkpoint['model_state_dict']
    if 'roberta.embeddings.position_ids' in state_dict:
        del state_dict['roberta.embeddings.position_ids']

    model.load_state_dict(state_dict)
    model.to(device)
    model.eval()

    return model, model_name


# ---------------------------------------------------------------------------
def get_ner_model(
        checkpoint_fh: BinaryIO,
        device: torch.device) -> Tuple[Any, str, PreTrainedTokenizer]:
    """
    Instatiate predictive NER model from checkpoint

    Supports both new format (weights_only=True, metrics as dicts)
    and old format (weights_only=False, metrics as NamedTuples).

    Params:
    `checkpoint_fh`: Model checkpoint filehandle
    `device`: The `torch.device` to use

    Return:
    Model instance from checkpoint, model name, and tokenizer
    """

    # Try new format first (weights_only=True) - version-resilient
    try:
        checkpoint = torch.load(checkpoint_fh, map_location=device, weights_only=True)
    except (pickle.UnpicklingError, RuntimeError, TypeError) as e:
        # Fall back to old format (weights_only=False) - for legacy checkpoints
        checkpoint_fh.seek(0)  # Reset file pointer
        checkpoint = torch.load(checkpoint_fh, map_location=device, weights_only=False)

    model_name = checkpoint['model_name']
    model = ner_classifier.from_pretrained(model_name,
                                           id2label=ID2NER_TAG,
                                           label2id=NER_TAG2ID)

    # Remove incompatible keys for newer transformers versions
    state_dict = checkpoint['model_state_dict']
    if 'roberta.embeddings.position_ids' in state_dict:
        del state_dict['roberta.embeddings.position_ids']

    model.load_state_dict(state_dict)
    model.to(device)
    model.eval()

    tokenizer = AutoTokenizer.from_pretrained(model_name)

    return model, model_name, tokenizer


# ---------------------------------------------------------------------------
def make_filenames(out_dir: str) -> Tuple[str, str]:
    """
    Make output filename

    Parameters:
    `out_dir`: Output directory to be included in filename

    Return: Tuple['{out_dir}/checkpt.pt', '{out_dir}/train_stats.csv']
    """

    return os.path.join(out_dir,
                        'checkpt.pt'), os.path.join(out_dir, 'train_stats.csv')


# ---------------------------------------------------------------------------
def test_make_filenames() -> None:
    """ Test make_filenames """

    assert make_filenames('out/scibert') == ('out/scibert/checkpt.pt',
                                             'out/scibert/train_stats.csv')


# ---------------------------------------------------------------------------
def save_model(model: Any, model_name: str, train_metrics: Metrics,
               val_metrics: Metrics, filename: str) -> None:
    """
    Save model checkpoint, epoch, and F1 score to file

    Saves metrics as dicts instead of NamedTuples for PyTorch version
    compatibility (enables weights_only=True loading).

    Parameters:
    `model`: Model to save
    `model_name`: Model HuggingFace name
    `train_metrics`: Metrics on training set of best epoch
    `val_metrics`: Metrics on validation set of best epoch
    `filename`: Name of file for saving model
    """

    torch.save(
        {
            'model_state_dict': model.state_dict(),
            'model_name': model_name,
            'train_metrics': {
                'precision': float(train_metrics.precision),
                'recall': float(train_metrics.recall),
                'f1': float(train_metrics.f1),
                'loss': float(train_metrics.loss)
            },
            'val_metrics': {
                'precision': float(val_metrics.precision),
                'recall': float(val_metrics.recall),
                'f1': float(val_metrics.f1),
                'loss': float(val_metrics.loss)
            }
        }, filename)


# ---------------------------------------------------------------------------
def save_train_stats(df: pd.DataFrame, filename: str) -> None:
    """
    Save training performance metrics to file

    Parameters:
    `df`: Training stats dataframe
    `filename`: Name of file for saving dataframe
    """

    df.to_csv(filename, index=False)


# ---------------------------------------------------------------------------
def save_metrics(model_name: str, metrics: Metrics, filename: str) -> None:
    """
    Save test metrics to csv file

    Parameters:
    `model_name`: Name of model
    `metrics`: A `Metrics` NamedTuple
    `filename`: Output file name
    """

    with open(filename, 'wt') as fh:
        print('model,precision,recall,f1,loss', file=fh)
        print(f'{model_name},{metrics.precision},{metrics.recall},',
              f'{metrics.f1},{metrics.loss}',
              sep='',
              file=fh)


# ---------------------------------------------------------------------------
if __name__ == '__main__':
    sys.exit('This file is a module, and is not meant to be run.')
