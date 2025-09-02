"""
Security Master Building Module
==============================

This module provides functionality to build CRSP/Compustat security master files 
from raw CRSP data. The security master is required by the ESG mappers to link 
ESG provider identifiers to GVKEY.

Main Functions:
- build_security_master(): Build complete security master from raw CRSP files

Usage:
    from security_master.build_security_master import build_security_master
    
    success = build_security_master()
"""
