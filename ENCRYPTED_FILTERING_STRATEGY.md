# Strategie de Filtrare a Mesajelor Criptate în EBS

## Problema

Sistemul actual de filtrare (Event-Based System cu Apache Storm) permite:
- Primirea publicațiilor complete la nivel de BrokerBolt
- Accesul la conținutul mesajelor pentru a efectua filtrarea pe subscripții
- Transmiterea mesajelor către NotifierBolt doar dacă se potrivesc criterii

**Dezavantaj:** Pentru a verifica dacă un mesaj corespunde unei subscripții, BrokerBolt trebuie să acceseze conținutul publicației și al subscripției, compromițând confidențialitatea datelor sensibile.

## Soluție: Filtrare Criptată la Nivel de Metadate

### 1. Arhitectura Generală

```
Publisher → PublicationCrypted (data + encrypted fields + metadata) → BrokerBolt
           ↓
       SubscriptionCrypted (encrypted criteria) → BrokerBolt
           ↓
       Filtrare pe METADATE doar (fără decriptare)
           ↓
       NotifierBolt → Subscriber (decriptare locală)
```

### 2. Modificări la Protocol Buffers

#### Publication.proto - Noua Versiune
```protobuf
syntax = "proto3";

message Publication {
    // Metadate publice (nesecurizate, pentru filtrare)
    string stationid = 1;
    string city = 2;
    
    // Date criptate (securizate)
    string encrypted_temp = 3;        // temperatura criptată
    string encrypted_wind = 4;        // vânt criptat
    
    // Indici de criptare/tag de autentificare
    string encryption_tag = 5;        // HMAC/GCM tag
    string encryption_key_id = 6;     // ID cheie de criptare
    
    // Metadate pentru filtrare criptată
    bytes encrypted_hash = 7;         // Hash criptat al valorilor sensibile
}
```

#### Subscription.proto - Noua Versiune
```protobuf
syntax = "proto3";

message Subscription {
    message FieldSubscription {
        string key = 1;                    // Câmp (stationid, city)
        string operator = 2;               // Operator (=, !=, etc)
        string value = 3;                  // Valoare plaintext
    }
    
    message EncryptedFieldSubscription {
        string key = 1;                    // Câmp (encrypted_temp, encrypted_wind)
        string operator = 2;               // Operator special: "~range~" sau "~match~"
        bytes encrypted_criteria = 3;      // Criterii criptate
        string encryption_key_id = 4;      // ID cheie corespunzătoare
    }

    repeated FieldSubscription fieldSubscriptions = 1;              // Criterii publice
    repeated EncryptedFieldSubscription encryptedSubscriptions = 2; // Criterii criptate
}
```

### 3. Implementare în BrokerBolt

```java
public class BrokerBolt extends BaseRichBolt {
    
    private HashMap<String, EncryptionManager> keyManagers;  // Per subscriber
    
    // Filtrare DOAR pe metadate publice
    boolean matches_public_criteria(Publication pub, List<FieldSubscription> subs) {
        for (FieldSubscription fs : subs) {
            if (!evaluate_operator(pub, fs)) {
                return false;
            }
        }
        return true;
    }
    
    // Filtrare pe date criptate - NU SE DECRIPTEAZĂ
    boolean matches_encrypted_criteria(Publication pub, 
                                       List<EncryptedFieldSubscription> encSubs,
                                       String subscriberId) {
        for (EncryptedFieldSubscription efs : encSubs) {
            // Opțiunea 1: Homomorphic encryption
            // Efectuează operații direct pe date criptate
            if (!evaluate_encrypted_operator(pub, efs)) {
                return false;
            }
            
            // Opțiunea 2: Order-preserving encryption
            // Compară ordinul valorilor criptate (fără decriptare)
            if (!compare_encrypted_values(pub, efs, subscriberId)) {
                return false;
            }
        }
        return true;
    }
    
    public void execute(Tuple input) {
        try {
            Publication pub = Publication.parseFrom(input.getBinaryByField("publication_data"));
            
            for (String boltId : subscriptions.keySet()) {
                List<List<FieldSubscription>> publicSubs = subscriptions.get(boltId);
                
                for (List<FieldSubscription> subList : publicSubs) {
                    // Pasul 1: Verifică criterii pe METADATE publice
                    if (!matches_public_criteria(pub, subList)) {
                        continue;  // Nu se potrivește
                    }
                    
                    // Pasul 2: Dacă există subscripții criptate, 
                    // verifică pe date criptate fără decriptare
                    List<EncryptedFieldSubscription> encSubs = 
                        get_encrypted_subscriptions(boltId);
                    
                    if (encSubs != null && !matches_encrypted_criteria(pub, encSubs, boltId)) {
                        continue;  // Nu se potrivește pe criterii criptate
                    }
                    
                    // EMIT: Trimitere la Notifier (mesaj încă criptat)
                    Stats.match_number++;
                    collector.emit("notifier" + boltId, 
                                   new Values((Object) pub.toByteArray()));
                }
            }
        } catch (InvalidProtocolBufferException e) {
            System.out.println(e);
        }
    }
}
```

### 4. Tehnici de Criptare Compatibile cu Filtrarea

#### A. **Order-Preserving Encryption (OPE)**
- **Principiu:** Criptarea conservă ordinul valorilor
- **Avantaj:** Permite comparații (<, >, <=, >=) fără decriptare
- **Dezavantaj:** Compromisuri de securitate; poate fi atacat prin análiza frecvenței

```java
class OPEEncryption {
    // Encrypt sensibil la ordine: criptă -> decriptă păstrează ordine
    public static byte[] encrypt_ope(String value, SecretKey key) {
        // AES OPE - valori numeric se mapează în spațiu criptat preservând ordine
        return performOPE(value.getBytes(), key);
    }
    
    // Comparație directă pe text criptat
    public static boolean compare_encrypted_ope(byte[] enc1, byte[] enc2, String operator) {
        // enc1 < enc2 (criptat) ⟺ plaintext(enc1) < plaintext(enc2)
        switch(operator) {
            case ">": return enc1 > enc2;  // Comparație lexicografică
            case "<": return enc1 < enc2;
            case ">=": return enc1 >= enc2;
            case "<=": return enc1 <= enc2;
        }
        return false;
    }
}
```

#### B. **Homomorphic Encryption (HE)**
- **Principiu:** Permite operații matematice pe date criptate
- **Avantaj:** Securitate maximă; nu expune ordine
- **Dezavantaj:** Foarte costisitor computațional

```java
class HomomorphicEncryption {
    // Evaluează expresii direct pe text criptat
    // Exp: avg(encrypted_values) fără decriptare
    public static CiphertextBigInt evaluate_average_encrypted(
            List<CiphertextBigInt> encryptedTemps) {
        CiphertextBigInt sum = encryptedTemps.get(0);
        for (int i = 1; i < encryptedTemps.size(); i++) {
            sum = sum.add(encryptedTemps.get(i));  // Adunare pe date criptate
        }
        // Rezultat: sum criptat = add(plaintext values) criptat
        return sum;
    }
}
```

#### C. **Searchable Encryption (SE)**
- **Principiu:** Hash criptat al valorilor permite potriviri exacte
- **Avantaj:** Rapid; permitere potriviri exacte fără decriptare
- **Dezavantaj:** Doar pentru egalitate (=, !=)

```java
class SearchableEncryption {
    public static byte[] trapdoor_for_value(String value, SecretKey key) {
        // Generează "trapdoor" criptat pentru valoare
        // Permite verificare: trapdoor(value) == SE.encrypt(message, value)
        HMac hmac = new HMac(new SHA256Digest());
        hmac.init(new KeyParameter(key.getEncoded()));
        hmac.update(value.getBytes(), 0, value.length());
        byte[] result = new byte[hmac.getMacSize()];
        hmac.doFinal(result, 0);
        return result;
    }
    
    // Verificare: Se.match(pub.encrypted_temp_hash, trapdoor(criteria))
    public static boolean match_encrypted(byte[] pubHash, byte[] trapdoor) {
        return Arrays.equals(pubHash, trapdoor);
    }
}
```

### 5. Exemplu de Workflow Complet

#### Scenariul: Subscriber vrea temperatura > 25°C de la București, fără ca BrokerBolt să vadă valoarea

**Pasul 1: Publisher criptează**
```java
Publication pub = new Publication()
    .setStationid("STN001")
    .setCity("Bucharest")  // Public - pentru filtrare
    .setEncrypted_temp(AES_GCM.encrypt("28.5", key))  // Criptat
    .setEncrypted_wind(AES_GCM.encrypt("5.2", key))   // Criptat
    .setEncryption_tag("GCM_AUTH_TAG");
```

**Pasul 2: Subscriber se abonează**
```java
Subscription sub = new Subscription()
    // Criterii publice
    .addFieldSubscription(new FieldSubscription("city", "=", "Bucharest"))
    // Criterii criptate: Temp > 25°C
    .addEncryptedSubscription(new EncryptedFieldSubscription(
        "encrypted_temp",
        "~gt_25~",  // Operator special pentru "> 25"
        OPE.encrypt("25", keySubscriber),
        "KEY_ID_001"
    ));
```

**Pasul 3: BrokerBolt filtrează (NU DECRIPTEAZĂ)**
```
1. Check: pub.city == "Bucharest" ✓ (public)
2. Check: OPE.compare(pub.encrypted_temp, trapdoor(25), ">") ✓
   - Comparație direct pe text criptat!
3. EMIT la notifier ← Mesaj încă criptat
```

**Pasul 4: NotifierBolt → Subscriber (decriptează local)**
```java
// Doar subscriberului i se trimite mesajul
Publication pub = Publication.parseFrom(message);
String temp = AES_GCM.decrypt(pub.getEncrypted_temp(), localKey);
System.out.println("Temperatura: " + temp);  // "28.5"
```

### 6. Matriz de Comparație Tehnici

| Tehnică | Operatori | Securitate | Viteză | Complexitate |
|---------|-----------|-----------|---------|-------------|
| **OPE** | <, >, <=, >= | ⚠️ Medie | ⭐⭐⭐⭐⭐ | Scăzută |
| **HE** | +, -, *, / | ⭐⭐⭐⭐⭐ | ⭐ | Foarte ridic. |
| **SE** | =, != | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | Medie |
| **Hibrid** | Toți | ⭐⭐⭐⭐ | ⭐⭐⭐ | Ridicată |

### 7. Recomandare Implementare

**Abordare Hibridă (cea mai practică):**

1. **Metadate publice** (stationid, city) → filtrare normală
2. **Valori numerice** (temp, wind) → OPE pentru comparații
3. **Valori exacte sensibile** → Searchable Encryption

```java
// În BrokerBolt
if (field.equals("temp") || field.equals("wind")) {
    // Foloseşte OPE pentru comparații numerice
    return OPEEncryption.compare_encrypted(...);
} else if (isSensitiveExactMatch(field)) {
    // Foloseşte SE pentru potriviri exacte
    return SearchableEncryption.match_encrypted(...);
} else {
    // Filtrare normală
    return normalComparison(...);
}
```

### 8. Beneficii

✅ **BrokerBolt nu vede niciodată valorile sensibile** (temp, wind)
✅ **Filtrarea se efectuează pe date criptate**
✅ **NotifierBolt primește mesaje criptate** (decriptează local doar subscriberii autorizați)
✅ **Performanță rezonabilă** cu abordare hibridă
✅ **Compatibil cu arquitectura Storm existentă**

### 9. Riscuri și Mitigări

| Risc | Mitigare |
|------|----------|
| Analiza frecvenței (OPE) | Adaugă padding/noise aleator |
| Replay attacks | Includeți timestamp și nonce în criptare |
| Key management | Utilizați serviciu dedicated (HSM, KMS) |
| Latență crescută | Cache rezultate; optimizați algoritmi |

