import re

def normalize_value(name: str, value: any) -> any:
    if value is None:
        return None
    
    if isinstance(value, str):
        value = value.strip()
        
        if not value:
            return value
        
        date_patterns = [
            r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})',
            r'(\d{1,2})[-/](\d{1,2})[-/](\d{4})',
            r'(\d{1,2})\.(\d{1,2})\.(\d{4})',
            r'(\d{4})\.(\d{1,2})\.(\d{1,2})'
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, value)
            if match:
                groups = match.groups()
                if len(groups) == 3:
                    try:
                        if len(groups[0]) == 4:
                            year, month, day = groups[0], groups[1], groups[2]
                        else:
                            if len(groups[2]) == 4:
                                day, month, year = groups[0], groups[1], groups[2]
                            else:
                                continue
                        
                        year = int(year)
                        month = int(month)
                        day = int(day)
                        
                        if 1 <= month <= 12 and 1 <= day <= 31:
                            iso_date = f"{year:04d}-{month:02d}-{day:02d}"
                            return iso_date
                    except (ValueError, IndexError):
                        pass
        
        amount_pattern = r'[\d,]+\.?\d*'
        if re.search(amount_pattern, value):
            normalized = re.sub(r',', '', value)
            return normalized
    
    return value
