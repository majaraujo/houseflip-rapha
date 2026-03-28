"""App-level constants and configuration."""

APP_TITLE = "Houseflip"
APP_ICON = ":material/home_work:"

# Color palette
COLOR_OPPORTUNITY_GOOD = "#22c55e"   # green — good deal
COLOR_OPPORTUNITY_BAD = "#ef4444"    # red — overpriced
COLOR_NEUTRAL = "#94a3b8"

# Listing type options (display label -> model value)
LISTING_TYPE_OPTIONS = {
    "Venda": "venda",
    "Aluguel": "aluguel",
}

# Property type options
PROPERTY_TYPE_OPTIONS = {
    "Apartamento": "apartamento",
    "Casa": "casa",
    "Terreno": "terreno",
    "Comercial": "comercial",
}

# Source options
SOURCE_OPTIONS = {
    "ZapImóveis": "zapimoveis",
    "VivaReal": "vivareal",
    "OLX": "olx",
    "Chaves na Mão": "chavesnamao",
    "Quinto Andar": "quintoandar",
}
