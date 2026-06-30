"""
Fix Windows-saved Keras model for Linux (Railway) compatibility.
The .keras weights HDF5 has backslash-separated group names (Windows artifact).
This converts them to forward-slash-separated nested groups that Linux Keras expects.

Run once: python fix_model_paths.py
Requires: pip install h5py
"""
import zipfile
import h5py
import io
import shutil

MODEL_PATH = "exercise_lstm_model.keras"

def fix_win_weights(model_path):
    print("Reading model file...")
    with zipfile.ZipFile(model_path, 'r') as zf:
        metadata = zf.read('metadata.json')
        config   = zf.read('config.json')
        old_data = io.BytesIO(zf.read('model.weights.h5'))

    print("Converting weight paths...")
    new_data = io.BytesIO()
    with h5py.File(old_data, 'r') as src:
        with h5py.File(new_data, 'w') as dst:
            # Copy root attributes
            for k, v in src.attrs.items():
                dst.attrs[k] = v

            def copy_dataset(name, obj):
                if not isinstance(obj, h5py.Dataset):
                    return
                new_path = name.replace('\\', '/')
                parent = '/'.join(new_path.split('/')[:-1])
                if parent:
                    dst.require_group(parent)
                dst.create_dataset(new_path, data=obj[()], dtype=obj.dtype)
                for k, v in obj.attrs.items():
                    dst[new_path].attrs[k] = v

            def copy_group_attrs(name, obj):
                if not isinstance(obj, h5py.Group):
                    return
                new_path = name.replace('\\', '/')
                if new_path in dst:
                    for k, v in obj.attrs.items():
                        dst[new_path].attrs[k] = v

            src.visititems(copy_dataset)
            src.visititems(copy_group_attrs)

            # Keras 2.15 on Linux expects an empty 'vars' group at EVERY
            # layer level, even for composite layers (Bidirectional, LSTM)
            # that store their weights in sub-layers. Add them where missing.
            layer_groups = []
            def find_layer_groups(name, obj):
                if not isinstance(obj, h5py.Group):
                    return
                # Any group under 'layers/' that is not 'vars' itself
                parts = name.split('/')
                if parts[0] == 'layers' and parts[-1] != 'vars':
                    layer_groups.append(name)
            dst.visititems(find_layer_groups)

            for grp_path in layer_groups:
                vars_path = grp_path + '/vars'
                if vars_path not in dst:
                    dst.require_group(vars_path)
                    print(f"  Added empty vars: {vars_path}")

    new_data.seek(0)

    print("Backing up original model...")
    shutil.copy(model_path, model_path + '.bak')

    print("Writing fixed model...")
    with zipfile.ZipFile(model_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('metadata.json', metadata)
        zf.writestr('config.json', config)
        zf.writestr('model.weights.h5', new_data.read())

    # Verify
    with zipfile.ZipFile(model_path, 'r') as zf:
        with h5py.File(io.BytesIO(zf.read('model.weights.h5')), 'r') as hf:
            paths = []
            hf.visititems(lambda n, o: paths.append(n) if isinstance(o, h5py.Dataset) else None)
        print(f"\nVerification — sample fixed paths:")
        for p in paths[:6]:
            print(f"  {p}")
        backslash_count = sum(1 for p in paths if '\\' in p)
        print(f"\nBackslash paths remaining: {backslash_count} (should be 0)")

    print("\nDone! Now run:")
    print("  git add backend/ml/exercise_lstm_model.keras")
    print("  git commit -m 'Fix model weights: convert Windows paths to Linux-compatible'")
    print("  git push koyeb deployment:main")

fix_win_weights(MODEL_PATH)
