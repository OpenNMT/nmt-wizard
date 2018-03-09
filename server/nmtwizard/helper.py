import random
import json
import uuid

adjectives = ["Able", "Acceptable", "Accurate", "Acidic", "Active", "Actual", "Additional",
    "Administrative", "Adult", "Afternoon", "Aggressive", "Agreeable", "Alive", "Alternative",
    "Amazing", "Ambitious", "Ancient", "Angry", "Annual", "Another", "Anxious", "Appropriate",
    "Asleep", "Attractive", "Automatic", "Available", "Aware", "Bald", "Basic", "Beautiful", "Best",
    "Bewildered", "Big", "Bitter", "Black", "Blue", "Boring", "Born", "Brave", "Breezy", "Brief",
    "Bright", "Brilliant", "Broad", "Brown", "Bumpy", "Busy", "Calm", "Capable", "Capital", "Careful",
    "Cheap", "Chemical", "Chicken", "Chilly", "Choice", "Chubby", "Civil", "Classic", "Clean", "Clear",
    "Clever", "Clumsy", "Cold", "Colossal", "Comfortable", "Commercial", "Common", "Competitive",
    "Complete", "Complex", "Comprehensive", "Confident", "Conscious", "Consistent", "Constant",
    "Content", "Cool", "Correct", "Crazy", "Creamy", "Creative", "Critical", "Cuddly", "Cultural",
    "Curious", "Current", "Curved", "Cute", "Damaged", "Damp", "Dane", "Dangerous", "Dark",
    "Dazzling", "Deafening", "Dear", "Decent", "Deep", "Defeated", "Delicious", "Delightful",
    "Desperate", "Different", "Difficult", "Direct", "Distinct", "Double", "Dramatic", "Drunk", "Dry",
    "Eager", "Early", "East", "Eastern", "Easy", "Economy", "Educational", "Effective", "Efficient",
    "Electrical", "Electronic", "Elegant", "Embarrassed", "Emergency", "Emotional", "Empty", "Entire",
    "Environmental", "Equal", "Equivalent", "Even", "Evening", "Exact", "Excellent", "Exciting",
    "Existing", "Expensive", "Expert", "Express", "Extension", "External", "Extra", "Extreme", "Faint",
    "Fair", "Faithful", "False", "Familiar", "Famous", "Fancy", "Fast", "Fat", "Federal", "Fierce",
    "Final", "Financial", "Fine", "Finnish", "Firm", "First", "Fit", "Flabby", "Flaky", "Flat", "Fluffy",
    "Foreign", "Formal", "Former", "Free", "Freezing", "Frequent", "Fresh", "Friendly", "Full", "Fun",
    "Funny", "Future", "Game", "General", "Gentle", "Gifted", "Gigantic", "Glad", "Glamorous", "Glass",
    "Global", "Gold", "Good", "Gorgeous", "Grand", "Gray", "Greasy", "Great", "Greek", "Green", "Gross",
    "Grumpy", "Guilty", "Hallowed", "Handsome", "Happy", "Hard", "Harsh", "Head", "Healthy", "Heavy",
    "Helpful", "Helpless", "High", "Hissing", "Historical", "Hollow", "Home", "Honest", "Hot", "House",
    "Howling", "Huge", "Human", "Hungry", "Icy", "Ideal", "Immediate", "Immense", "Important",
    "Impossible", "Impressive", "Independent", "Individual", "Inevitable", "Inexpensive", "Informal",
    "Initial", "Inner", "Inside", "Intelligent", "Interesting", "Internal", "International", "Itchy",
    "Jealous", "Joint", "Jolly", "Juicy", "Junior", "Just", "Key", "Kind", "Kitchen", "Known", "Large",
    "Last", "Late", "Lazy", "Leading", "Least", "Leather", "Left", "Legal", "Lemon", "Level",
    "Life", "Little", "Live", "Lively", "Living", "Local", "Logical", "Lonely", "Long", "Loose", "Lost",
    "Loud", "Low", "Lower", "Lucky", "Mad", "Magnificent", "Main", "Major", "Mango", "Massive", "Master",
    "Maximum", "Mealy", "Mean", "Medical", "Medium", "Melodic", "Melted", "Mental", "Microscopic",
    "Middle", "Miniature", "Minimum", "Minor", "Minute", "Mission", "Mobile", "Modern", "Moldy", "Money",
    "More", "Mother", "Motor", "Muscular", "Mushy", "Mysterious", "Narrow", "National", "Native",
    "Natural", "Neat", "Necessary", "Negative", "Neither", "Nervous", "New", "Next", "Nice", "Noisy",
    "Normal", "North", "Novel", "Numerous", "Nutritious", "Nutty", "Obedient", "Objective", "Obnoxious",
    "Obvious", "Odd", "Official", "Old", "Only", "Open", "Opening", "Opposite", "Orange", "Ordinary", "Original",
    "Otherwise", "Outside", "Overall", "Particular", "Party", "Past", "Patient", "Perfect", "Personal", "Petite",
    "Physical", "Pitiful", "Plain", "Plane", "Plastic", "Pleasant", "Plenty", "Plump", "Polite", "Political",
    "Poor", "Popular", "Positive", "Possible", "Potential", "Powerful", "Practical", "Prehistoric", "Pretty",
    "Previous", "Prickly", "Primary", "Prior", "Private", "Prize", "Professional", "Proper", "Proud",
    "Psychological", "Public", "Puny", "Pure", "Purple", "Purring", "Putrid", "Quaint", "Quick", "Quiet",
    "Rancid", "Rapid", "Rare", "Raspy", "Real", "Realistic", "Reasonable", "Red", "Refined", "Regular",
    "Relative", "Relevant", "Remarkable", "Remote", "Resident", "Responsible", "Rhythmic", "Rich", "Right", "Ripe",
    "Rotten", "Rough", "Round", "Routine", "Royal", "Sad", "Safe", "Salmon", "Salt", "Salty", "Scared", "Scary",
    "Scrawny", "Screeching", "Scruffy", "Secret", "Secure", "Select", "Senior", "Sensitive", "Separate", "Serious",
    "Severe", "Shaggy", "Shallow", "Shapely", "Sharp", "Short", "Shrilling", "Shy", "Sick", "Signal",
    "Significant", "Silly", "Silver", "Similar", "Simple", "Single", "Skinny", "Slight", "Slimy", "Slow", "Small",
    "Smart", "Smooth", "Soft", "Solid", "Sour", "South", "Southern", "Spare", "Special",
    "Specialist", "Specific", "Spicy", "Spiritual", "Spoiled", "Square", "Squeaking", "Stale", "Standard", "Steep",
    "Sticky", "Still", "Stock", "Stocky", "Straight", "Strange", "Street", "Strict", "Strong", "Substantial",
    "Successful", "Sudden", "Sufficient", "Suitable", "Super", "Sure", "Suspicious", "Sweet", "Swift", "Swimming",
    "Tall", "Tangy", "Tart", "Tasteless", "Tasty", "Technical", "Teeny", "Temporary", "Tender", "Terrible",
    "Thankful", "Thick", "Thin", "Thoughtless", "Thundering", "Tight", "Tinkling", "Tiny", "Top",
    "Tough", "Traditional", "Training", "Tricky", "True", "Typical", "Ugly", "Uneven", "Unfair",
    "Unhappy", "Unimportant", "Uninterested", "Unique", "United", "Unkempt", "Unlikely", "Unsightly", "Unusual",
    "Upper", "Upset", "Useful", "Usual", "Valuable", "Various", "Vast", "Victorious", "Visible", "Visual", "Wailing",
    "Warm", "Waste", "Weak", "Weekly", "Weird", "West", "Western", "Wet", "Whining", "Whispering", "White", "Wide",
    "Wild", "Willing", "Winter", "Wise", "Witty", "Wonderful", "Wooden", "Work", "Working", "Worried", "Worth",
    "Yellow", "Young", "Zealous"]
nouns = ["Aardvark", "Aardwolf", "Albatross", "Alligator", "Alpaca", "Anaconda", "Anglerfish", "Ant", "Anteater",
    "Antelope", "Antlion", "Ape", "Aphid", "Armadillo", "Asp", "Axolotl", "Baboon", "Badger", "Bandicoot", "Barnacle",
    "Barracuda", "Basilisk", "Bass", "Bat", "Beaver", "Bedbug", "Bee", "Beetle", "Bison", "Blackbird", "Boa", "Bobcat",
    "Bobolink", "Bonobo", "Booby", "Bovid", "Buffalo", "Bug", "Bull", "Butterfly", "Buzzard", "Camel", "Canid",
    "Capybara", "Cardinal", "Caribou", "Carp", "Caterpillar", "Catfish", "Catshark", "Centipede", "Cephalopod",
    "Chameleon", "Cheetah", "Chickadee", "Chimpanzee", "Chinchilla", "Chipmunk", "Cicada", "Clam", "Clownfish",
    "Coati", "Cobra", "Cockroach", "Cod", "Condor", "Constrictor", "Coral", "Cougar", "Cow", "Coyote", "Coypu",
    "Crab", "Crane", "Crawdad", "Crayfish", "Cricket", "Crocodile", "Crow", "Cuckoo", "Damselfly", "Deer", "Dhole",
    "Dingo", "Dodo", "Dolphin", "Dormouse", "Dove", "Dragon", "Dragonfly", "Eagle", "Earthworm", "Earwig", "Echidna",
    "Eel", "Egret", "Elephant", "Elk", "Emu", "Ermine", "Falcon", "Fennec", "Ferret", "Finch", "Firefly", "Fish",
    "Flamingo", "Flea", "Fly", "Flyingfish", "Fowl", "Fox", "Frog", "Gazelle", "Gecko", "Gerbil", "Gibbon", "Giraffe",
    "Goldfish", "Gopher", "Gorilla", "Grasshopper", "Grebe", "Grouse", "Guanaco", "Gull", "Guppy", "Haddock", "Halibut",
    "Hamster", "Hare", "Harrier", "Hawk", "Hedgehog", "Heron", "Herring", "Hippopotamus", "Hookworm", "Hornet", "Hoverfly",
    "Human", "Hummingbird", "Hyena", "Hyrax", "Ibis", "Iguana", "Jacana", "Jackal", "Jaguar", "Jay", "Jellyfish", "Kangaroo",
    "Kingfisher", "Kite", "Kiwi", "Koala", "Koi", "Krill", "Ladybug", "Lamprey", "Landfowl", "Lapwing", "Lark", "Leech",
    "Lemming", "Lemur", "Leopard", "Leopon", "Limpet", "Lion", "Lionfish", "Lizard", "Llama", "Lobster", "Locust", "Loon",
    "Loris", "Louse", "Lungfish", "Lynx", "Macaw", "Mackerel", "Magpie", "Mallard", "Manatee", "Mandrill", "Marlin",
    "Marmoset", "Marmot", "Marsupial", "Marten", "Mastodon", "Maya", "Meadowlark", "Meerkat", "Mink", "Minnow", "Mite",
    "Mockingbird", "Mole", "Mollusk", "Mongoose", "Monkey", "Moose", "Mosquito", "Moth", "Mouse", "Mule", "Muskox", "Narwhal",
    "Needlefish", "Newt", "Nighthawk", "Nightingale", "Numbat", "Ocelot", "Octopus", "Okapi", "Olingo", "Opossum", "Orangutan",
    "Orca", "Oribi", "Ostrich", "Otter", "Owl", "Ox", "Panda", "Panther", "Parakeet", "Parrot", "Parrotfish", "Partridge",
    "Peacock", "Peafowl", "Pelican", "Penguin", "Perch", "Pheasant", "Pig", "Pike", "Pinniped", "Piranha", "Planarian",
    "Platypus", "Pony", "Porcupine", "Porpoise", "Possum", "Prawn", "Primate", "Ptarmigan", "Puffin", "Puma", "Python",
    "Quail", "Quelea", "Quetzal", "Quokka", "Raccoon", "Rat", "Rattlesnake", "Raven", "Reindeer", "Reptile", "Rhinoceros",
    "Roadrunner", "Rodent", "Rook", "Rooster", "Roundworm", "Sailfish", "Salamander", "Salmon", "Sawfish", "Scallop",
    "Scorpion", "Seahorse", "Serval", "Shrew", "Shrimp", "Silkworm", "Silverfish", "Skink", "Skunk", "Sloth", "Slug", "Smelt",
    "Snail", "Snipe", "Sole", "Sparrow", "Spider", "Spoonbill", "Squid", "Squirrel", "Starfish", "Stingray", "Stoat", "Stork",
    "Sturgeon", "Swallow", "Swan", "Swift", "Swordfish", "Swordtail", "Tahr", "Takin", "Tapir", "Tarantula", "Tarsier",
    "Termite", "Tern", "Thrush", "Tick", "Tiger", "Tiglon", "Titi", "Toad", "Tortoise", "Toucan", "Trout", "Tuna", "Turtle",
    "Tyrannosaurus", "Unicorn", "Urial", "Vaquita", "Viper", "Vixen", "Voalavoanala", "Vole", "Vulture", "Wallaby", "Walrus",
    "Warbler", "Wasp", "Waterbuck", "Weasel", "Whale", "Whippet", "Whitefish", "Wildcat", "Wildebeest", "Wildfowl", "Wolf",
    "Wolverine", "Wombat", "Woodchuck", "Woodpecker", "Worm", "Wren", "Xerinae", "Yak", "Zebra", "Zebu", "Zorilla",
    "Alfalfa", "Allium", "Alyssum", "Amaranth", "Anemone", "Anise", "Apple", "Apricot", "Artemisia", "Artichoke", "Arugula",
    "Asparagus", "Aster", "Astilbe", "Aubergine", "Avocado", "Azuki", "Banana", "Basil", "Bean", "Beet", "Beetroot",
    "Bellflower", "Blackberry", "Blackcurrant", "Blanketflower", "Blueberry", "Bougainvillea", "Boysenberry", "Broccoli",
    "Broom", "Cabbage", "Camellia", "Caraway", "Carrot", "Catmint", "Cauliflower", "Celeriac", "Celery", "Chamomile", "Chard",
    "Cheddar", "Cherry", "Chickpea", "Chives", "Chrysanthemum", "Cilantro", "Clematis", "Coconut", "Columbine", "Coneflower",
    "Coreopsis", "Coriander", "Cosmos", "Courgette", "Crocus", "Cucumber", "Cyclamen", "Dahlia", "Daikon", "Delicata", "Delphinium",
    "Dill", "Eggplant", "Endive", "Fennel", "Fig", "Foxglove", "Frisee", "Garlic", "Gayfeather", "Geranium", "Ginger", "Gladiolus",
    "Globeflower", "Grape", "Grapefruit", "Habanero", "Hollyhock", "Honeysuckle", "Hosta", "Hyacinth", "Hydrangea", "Impatien",
    "Iris", "Jicama", "Kale", "Kerria", "Kiwifruit", "Kohlrabi", "Lamium", "Lantana", "Larkspur", "Lavender", "Leek", "Lemon",
    "Lemongrass", "Lentil", "Lettuce", "Lilac", "Lime", "Lobelia", "Loosestrife", "Lupine", "Lychee", "Mandarin", "Mangetout",
    "Mango", "Marigold", "Marjoram", "Marrow", "Melon", "Mushroom", "Narcissus", "Nasturtium", "Nectarine", "Nicotiana", "Okra",
    "Oleander", "Onion", "Onions", "Orange", "Oregano", "Pansy", "Papaya", "Paprika", "Parrot", "Parsley", "Parsnip", "Passion",
    "Pea", "Peach", "Pear", "Peony", "Pepper", "Petunias", "Pineapple", "Pinks", "Plum", "Poppy", "Potato", "Primrose", "Pumpkin",
    "Quandong", "Quince", "Radicchio", "Radish", "Raspberry", "Rhododendron", "Rhubarb", "Rosemary", "Rutabaga", "Sage", "Salsify",
    "Salvia", "Scabiosa", "Scallion", "Scilla", "Sedum", "Shallot", "Skirret", "Snowdrops", "Spinach", "Sprout", "Squash",
    "Strawberry", "Sunchokes", "Sweetcorn", "Taro", "Thyme", "Tomato", "Topinambur", "Tubers", "Tulip", "Turnip", "Vinca",
    "Wasabi", "Watercress", "Watermelon", "Wisteria", "Yam", "Yarrow", "Zucchini"]

def _generate_name():
    while True:
        name = random.choice(adjectives)+random.choice(nouns)
        if len(name) <= 14 :
            return name

def shallow_command_analysis(command):
    i = 0
    xx = 'xx'
    yy = 'yy'
    parent_task = None
    while i < len(command):
        if command[i] == '-m' and i+1 < len(command):
            parent_task = command[i+1]
            i += 1
        elif command[i] == '-c' and i+1 < len(command):
            config = json.loads(command[i+1])
            if not parent_task and "model" in config:
                parent_task = config["model"]
            if "source" in config:
                xx = config["source"]
            if "target" in config:
                yy = config["target"]
            i += 1
        i += 1
    return (xx+yy, parent_task)

def change_parent_task(command, task_id):
    i = 0
    while i < len(command):
        if command[i] == '-m' and i+1 < len(command):
            command[i+1] = task_id
            return
        i += 1
    command.insert(0, task_id)
    command.insert(0, '-m')

def _model_name_analysis(model):
    if model:
        struct = {}
        l = model.split("_")
        if len(l) < 4 or len(l)>5:
            return
        if len(l) == 5:
            struct["trid"] = l.pop(0)
        else:
            struct["trid"] = None
        struct["xxyy"] = l.pop(0)
        struct["name"] = l.pop(0)
        struct["nn"] = l.pop(0)
        usplit = struct["uuid"].split('-')
        if len(usplit) > 1:
            struct["uuid"] = usplit[0]
            struct["parent_uuid"] = usplit[-1]
        return struct

def build_task_id(content, xxyy, parent_task):
    # let us build a meaningful name for the task
    # name will be TRID_XXYY_NAME_NN_UUID(:UUID) with:
    # * TRID - the trainer ID
    # * XXYY - the language pair
    # * NAME - user provided or generated name 
    # * NN - the iteration (epoch) - automatically incremented for training task
    # * UUID - one or 2 parts - parent:child or child

    # first find nature of the task - train or not
    is_train = "train" in content["docker"]["command"]
    trid = 'XXXX'
    if 'trainer_id' in content and content['trainer_id']:
        trid = content['trainer_id']
    nn = 0
    name = content["name"] if "name" in content else None
    parent_uuid = ''
    if parent_task is not None:
        struct_name = _model_name_analysis(parent_task)
        if name is None and "name" in struct_name:
            name = struct_name["name"]
        if xxyy is None and "xxyy" in struct_name:
            xxyy = struct_name["xxyy"]
        if "uuid" in struct_name:
            parent_uuid = '-'+struct_name["uuid"][0:5]
        if "nn" in struct_name:
            nn = int(struct_name["nn"])

    if is_train:
        nn += 1
        if not name:
            name = _generate_name()

    the_uuid = str(uuid.uuid4()).replace("-","")

    task_id = '%s_%s_%s_%02d_%s' % (trid, xxyy, name, nn, the_uuid)
    task_id = task_id[0:41-len(parent_uuid)] + parent_uuid
    return task_id