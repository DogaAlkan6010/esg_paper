"""
ESG Data Mappers
================

This module provides mappers for various ESG data providers to map their 
identifiers (like OrgPermID, IssuerID) to GVKEY for linkage with Compustat data.

Available Mappers:
- RefinitivMapper: Maps Refinitiv OrgPermID to GVKEY
- MSCIMapper: Maps MSCI IssuerID to GVKEY

Usage:
    from esg_mappers.refinitiv_mapper import RefinitivMapper
    
    mapper = RefinitivMapper()
    matches, crosswalk = mapper.run("path/to/refinitiv_data.csv")
"""

from .base_mapper import BaseESGMapper
from .refinitiv_mapper import RefinitivMapper
from .msci_mapper import MSCIMapper
from .fmp_mapper import FMPMapper

__all__ = ["BaseESGMapper", "RefinitivMapper", "MSCIMapper", "FMPMapper"]
